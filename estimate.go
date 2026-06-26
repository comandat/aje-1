package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"sync"
	"time"
)

// initial_estimated_price = avg(first 3 competitor current prices)
//   - 20% (we undercut the competition)
//   - 25% (commissions, VAT, transport)
//   - 5% (breakage / dead-on-arrival loss)
//
// Applied multiplicatively, per the spec.
const (
	cheaperThanComp  = 0.20
	feesVATTransport = 0.25
	breakageLoss     = 0.05
)

// compProduct mirrors the competition-search-module output.
type compProduct struct {
	ProductName    string  `json:"productName"`
	CurrentPrice   float64 `json:"currentPrice"`
	OldPrice       float64 `json:"oldPrice"`
	Discount       float64 `json:"discount"`
	PromotionLabel string  `json:"promotionLabel"`
	DealType       string  `json:"dealType"`
	Rating         float64 `json:"rating"`
	ReviewsCount   int     `json:"reviewsCount"`
	ProductURL     string  `json:"productUrl"`
	ProductImage   string  `json:"productImage"`
}

type compCategory struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// CompetitionResult is one element of the array the module POSTs to our callback.
type CompetitionResult struct {
	ASIN       string         `json:"asin"`
	Products   []compProduct  `json:"products"`
	Categories []compCategory `json:"categories"`
}

// legacyCompetitor is the shape we persist in competition.visual_search, kept
// compatible with the historical records (note: price_current is the field the
// estimation reads).
type legacyCompetitor struct {
	URL            string  `json:"url"`
	Name           string  `json:"name"`
	Image          string  `json:"image"`
	Rating         float64 `json:"rating"`
	ReviewsCount   int     `json:"reviews_count"`
	PriceOld       float64 `json:"price_old"`
	PriceCurrent   float64 `json:"price_current"`
	Discount       float64 `json:"discount"`
	DealType       string  `json:"deal_type"`
	PromotionLabel string  `json:"promotion_label"`
	Position       int     `json:"position"`
}

func round2(x float64) float64 { return math.Round(x*100) / 100 }

func priceFromProducts(products []compProduct) float64 {
	var sum float64
	var n int
	for _, p := range products {
		if p.CurrentPrice > 0 {
			sum += p.CurrentPrice
			n++
		}
		if n == 3 {
			break
		}
	}
	if n == 0 {
		return 0
	}
	avg := sum / float64(n)
	return round2(avg * (1 - cheaperThanComp) * (1 - feesVATTransport) * (1 - breakageLoss))
}

// startEstimateAll kicks off estimation for every not-yet-estimated pallet and
// returns how many were launched. Work runs in the background (manifest
// downloads are slow) so the HTTP caller returns immediately.
func (a *App) startEstimateAll() int {
	skus := a.estimableSKUs()
	go func() {
		n := a.cfg.EstimateConcurrency
		if n < 1 {
			n = 1
		}
		sem := make(chan struct{}, n) // concurrent manifest pipelines (ESTIMATE_CONCURRENCY, default 20).
		var wg sync.WaitGroup
		for _, sku := range skus {
			wg.Add(1)
			go func(s string) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()
				if err := a.estimatePallet(s); err != nil {
					log.Printf("estimate %s: %v", s, err)
				}
			}(sku)
		}
		wg.Wait()
		log.Printf("estimate run finished: %d pallets launched", len(skus))
	}()
	return len(skus)
}

// estimableSKUs returns tomorrow's not-yet-estimated pallets only — the big
// button is "estimează paleții de mâine", so today's tab is never touched.
func (a *App) estimableSKUs() []string {
	tomorrow := time.Now().In(tzBucharest).AddDate(0, 0, 1).Format("2006-01-02")
	rows, err := a.db.Query(`SELECT sku, end_at FROM pallets WHERE estimate_status IN ('', 'error')`)
	if err != nil {
		log.Printf("estimableSKUs: %v", err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var sku, endAt string
		if err := rows.Scan(&sku, &endAt); err == nil && bucharestDate(endAt) == tomorrow {
			out = append(out, sku)
		}
	}
	return out
}

// estimatePallet resolves every ASIN of a pallet to an initial_estimated_price.
// Known ASINs come straight from Postgres; unknown ones are sent to the
// competition module, which calls onCompetitionCallback back asynchronously.
func (a *App) estimatePallet(sku string) error {
	if !a.estimationEnabled() {
		return errors.New("n8n postgres gateway not configured")
	}
	log.Printf("estimate %s: starting", sku)
	p, err := a.getPallet(sku)
	if err != nil {
		return err
	}
	if p == nil {
		return fmt.Errorf("pallet %s not found", sku)
	}
	if p.EstimateStatus == "done" {
		log.Printf("estimate %s: already done, skipping", sku)
		return nil
	}

	// Re-run: a previous partial/error pass left items unpriced (price 0, resolved).
	// Clear them so lookup + competition get another shot — the DB may have prices
	// now (saved by sibling pallets) and competition may match on retry.
	if p.EstimateStatus == "partial" || p.EstimateStatus == "error" {
		if r, err := a.db.Exec(`UPDATE pallet_items SET resolved=0, requested=0 WHERE sku=? AND price<=0`, sku); err != nil {
			log.Printf("estimate %s: reset unpriced: %v", sku, err)
		} else if n, _ := r.RowsAffected(); n > 0 {
			log.Printf("estimate %s: re-run, reset %d unpriced items", sku, n)
		}
	}

	if p.ItemsCount == 0 {
		log.Printf("estimate %s: downloading manifest (%s)", sku, p.ManifestSKU)
		if err := a.ensureItems(sku, p.ManifestSKU); err != nil {
			log.Printf("estimate %s: manifest error: %v", sku, err)
			a.setError(sku, err.Error())
			return err
		}
	}
	a.setStatus(sku, "in_progress")

	asins := a.palletASINs(sku)
	log.Printf("estimate %s: %d ASINs in manifest, looking up known prices", sku, len(asins))
	known, err := a.knownPrices(asins)
	if err != nil {
		log.Printf("estimate %s: knownPrices error: %v", sku, err)
		a.setError(sku, fmt.Sprintf("lookup prețuri: %v", err))
		return fmt.Errorf("postgres dedup: %w", err)
	}
	for asin, price := range known {
		a.setItemResolved(asin, price)
	}

	toRequest := a.unrequestedItems(sku)
	log.Printf("estimate %s: %d known, %d to request from competition module", sku, len(known), len(toRequest))
	if len(toRequest) > 0 {
		a.markRequested(toRequest)
		go a.triggerCompetition(toRequest)
	} else {
		log.Printf("estimate %s: all ASINs already known or requested, finalizing", sku)
	}

	a.finalizeIfReady(sku)
	return nil
}

func (a *App) ensureItems(sku, manifestSKU string) error {
	items, err := a.downloadManifest(manifestSKU)
	if err != nil {
		return err
	}
	if len(items) == 0 {
		return fmt.Errorf("manifest %s has no items", manifestSKU)
	}
	for _, it := range items {
		imgs, _ := json.Marshal(it.Images)
		_, err := a.db.Exec(`
INSERT INTO pallet_items (sku, asin, qty, image, title, description, images, brand, ean, category, subcategory, unit_weight)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(sku, asin) DO UPDATE SET qty=excluded.qty`,
			sku, it.ASIN, it.Qty, it.Image, it.Title, it.Description, string(imgs),
			it.Brand, it.EAN, it.Category, it.Subcategory, it.UnitWeight)
		if err != nil {
			return fmt.Errorf("insert item %s: %w", it.ASIN, err)
		}
	}
	return nil
}

func (a *App) palletASINs(sku string) []string {
	rows, err := a.db.Query(`SELECT asin FROM pallet_items WHERE sku=?`, sku)
	if err != nil {
		log.Printf("palletASINs %s: %v", sku, err)
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var asin string
		if err := rows.Scan(&asin); err == nil {
			out = append(out, asin)
		}
	}
	return out
}

// knownPrices asks the n8n lookup webhook which ASINs already have an
// initial_estimated_price in the central Postgres products table.
// Expected response: [{"asin":"B0..","price":12.34}, ...].
func (a *App) knownPrices(asins []string) (map[string]float64, error) {
	out := map[string]float64{}
	if len(asins) == 0 {
		return out, nil
	}
	log.Printf("knownPrices: sending %d ASINs to lookup webhook", len(asins))
	body, _ := json.Marshal(map[string][]string{"asins": asins})
	resp, err := a.http.Post(a.cfg.N8NLookupURL, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		// n8n returns 500 when the query finds no rows — treat as empty result, not an error.
		if strings.Contains(string(b), "No item to return was found") {
			log.Printf("knownPrices: webhook returned 500 (no rows in DB) — treating as 0 known prices")
			return out, nil
		}
		return nil, fmt.Errorf("lookup webhook status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	parsed, err := parseKnownPrices(b)
	if err != nil {
		// Log the raw shape so we can see what n8n actually sends.
		sample := string(b)
		if len(sample) > 300 {
			sample = sample[:300]
		}
		log.Printf("knownPrices: decode failed, raw response: %s", sample)
		return nil, err
	}
	log.Printf("knownPrices: %d of %d ASINs already have prices in DB", len(parsed), len(asins))
	return parsed, nil
}

// parseKnownPrices accepts the lookup webhook response in whatever shape n8n
// sends it: a JSON array [{asin,price},...], a single object {asin,price}, or a
// wrapper {"data":[...]}. Unknown ASIN keys are uppercased to match our items.
func parseKnownPrices(b []byte) (map[string]float64, error) {
	out := map[string]float64{}
	b = bytes.TrimSpace(b)
	if len(b) == 0 {
		return out, nil
	}
	type row struct {
		ASIN  string    `json:"asin"`
		Price flexFloat `json:"price"` // Postgres numeric may arrive as string
	}
	add := func(rs []row) {
		for _, r := range rs {
			if r.ASIN != "" {
				out[strings.ToUpper(r.ASIN)] = float64(r.Price)
			}
		}
	}
	switch b[0] {
	case '[':
		var rows []row
		if err := json.Unmarshal(b, &rows); err != nil {
			return nil, fmt.Errorf("lookup decode array: %w", err)
		}
		add(rows)
	case '{':
		// Try single row, then {data:[...]}.
		var single row
		if err := json.Unmarshal(b, &single); err == nil && single.ASIN != "" {
			add([]row{single})
			break
		}
		var wrapped struct {
			Data []row `json:"data"`
		}
		if err := json.Unmarshal(b, &wrapped); err == nil && len(wrapped.Data) > 0 {
			add(wrapped.Data)
			break
		}
		return nil, fmt.Errorf("lookup decode: unrecognized object shape")
	default:
		return nil, fmt.Errorf("lookup decode: unexpected response")
	}
	return out, nil
}

// unrequestedItems are this pallet's items that are still unresolved and have
// not already been sent to the competition module (possibly via another pallet).
func (a *App) unrequestedItems(sku string) []ManifestItem {
	rows, err := a.db.Query(`SELECT asin, image FROM pallet_items WHERE sku=? AND resolved=0 AND requested=0`, sku)
	if err != nil {
		log.Printf("unrequestedItems %s: %v", sku, err)
		return nil
	}
	defer rows.Close()
	var out []ManifestItem
	for rows.Next() {
		var it ManifestItem
		if err := rows.Scan(&it.ASIN, &it.Image); err == nil {
			out = append(out, it)
		}
	}
	return out
}

func (a *App) markRequested(items []ManifestItem) {
	for _, it := range items {
		if _, err := a.db.Exec(`UPDATE pallet_items SET requested=1 WHERE asin=?`, it.ASIN); err != nil {
			log.Printf("markRequested %s: %v", it.ASIN, err)
		}
	}
}

func (a *App) triggerCompetition(items []ManifestItem) {
	type mp struct {
		ASIN         string `json:"asin"`
		ProductImage string `json:"productimage"`
	}
	missing := make([]mp, 0, len(items))
	for _, it := range items {
		missing = append(missing, mp{ASIN: it.ASIN, ProductImage: it.Image})
	}
	payload := map[string]any{"body": map[string]any{"missingProducts": missing}}
	b, _ := json.Marshal(payload)

	log.Printf("competition: sending %d ASINs to %s", len(items), a.cfg.CompetitionURL)
	resp, err := a.http.Post(a.cfg.CompetitionURL, "application/json", bytes.NewReader(b))
	if err != nil {
		log.Printf("competition trigger: %v", err)
		return
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	log.Printf("competition: response status %d, body: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// onCompetitionCallback handles the async result POSTed by the module. It prices
// each ASIN, writes new products to Postgres, marks items resolved, and
// finalizes any pallet whose ASINs are now all resolved.
func (a *App) onCompetitionCallback(results []CompetitionResult) {
	priced, zero := 0, 0
	for _, r := range results {
		asin := strings.ToUpper(r.ASIN)
		price := priceFromProducts(r.Products)
		if price > 0 {
			priced++
			if meta := a.itemMetaByASIN(asin); meta != nil {
				if err := a.writeProduct(*meta, price, r); err != nil {
					log.Printf("postgres write %s: %v", asin, err)
				}
			}
		} else {
			zero++
			log.Printf("callback %s: 0 price from %d products", asin, len(r.Products))
		}
		// Resolve even at price 0 (no competitors found) so the pallet can finalize
		// — such items contribute 0 and flag the pallet as 'partial'.
		a.setItemResolved(asin, price)
	}
	log.Printf("callback batch: %d results — %d priced, %d zero", len(results), priced, zero)
	a.finalizeAllReady()
}

func (a *App) setItemResolved(asin string, price float64) {
	if _, err := a.db.Exec(`UPDATE pallet_items SET price=?, resolved=1 WHERE asin=?`, price, strings.ToUpper(asin)); err != nil {
		log.Printf("setItemResolved %s: %v", asin, err)
	}
}

func (a *App) itemMetaByASIN(asin string) *ManifestItem {
	const q = `SELECT asin, qty, image, title, description, images, brand, ean, category, subcategory, unit_weight
	           FROM pallet_items WHERE asin=? LIMIT 1`
	var it ManifestItem
	var imagesJSON string
	err := a.db.QueryRow(q, asin).Scan(&it.ASIN, &it.Qty, &it.Image, &it.Title, &it.Description,
		&imagesJSON, &it.Brand, &it.EAN, &it.Category, &it.Subcategory, &it.UnitWeight)
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("itemMetaByASIN %s: %v", asin, err)
		}
		return nil
	}
	json.Unmarshal([]byte(imagesJSON), &it.Images)
	return &it
}

// writeProduct hands a freshly-estimated product to the n8n save webhook, which
// persists it to products.products, products.translations and
// competition.visual_search.
func (a *App) writeProduct(meta ManifestItem, price float64, r CompetitionResult) error {
	competitors := make([]legacyCompetitor, 0, len(r.Products))
	for i, cp := range r.Products {
		competitors = append(competitors, legacyCompetitor{
			URL: cp.ProductURL, Name: cp.ProductName, Image: cp.ProductImage,
			Rating: cp.Rating, ReviewsCount: cp.ReviewsCount, PriceOld: cp.OldPrice,
			PriceCurrent: cp.CurrentPrice, Discount: cp.Discount, DealType: cp.DealType,
			PromotionLabel: cp.PromotionLabel, Position: i + 1,
		})
	}

	images := meta.Images
	if images == nil {
		images = []string{}
	}
	categories := r.Categories
	if categories == nil {
		categories = []compCategory{}
	}

	payload := map[string]any{
		"asin":        meta.ASIN,
		"price":       price,
		"brand":       meta.Brand,
		"ean":         meta.EAN,
		"unit_weight": meta.UnitWeight,
		"category":    meta.Category,
		"subcategory": meta.Subcategory,
		"title":       meta.Title,
		"description": meta.Description,
		"images":      images,
		"competitors": competitors,
		"categories":  categories,
	}
	b, _ := json.Marshal(payload)

	resp, err := a.http.Post(a.cfg.N8NSaveURL, "application/json", bytes.NewReader(b))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("save webhook status %d", resp.StatusCode)
	}
	return nil
}

// finalizeIfReady computes the pallet total once all its items are resolved.
func (a *App) finalizeIfReady(sku string) {
	var pending int
	if err := a.db.QueryRow(`SELECT COUNT(*) FROM pallet_items WHERE sku=? AND resolved=0`, sku).Scan(&pending); err != nil {
		log.Printf("finalizeIfReady %s: %v", sku, err)
		return
	}
	if pending > 0 {
		return
	}

	var total sql.NullFloat64
	var nItems, nPriced int
	if err := a.db.QueryRow(`
SELECT COALESCE(SUM(price*qty),0), COUNT(*), COALESCE(SUM(CASE WHEN price>0 THEN 1 ELSE 0 END),0)
FROM pallet_items WHERE sku=?`, sku).Scan(&total, &nItems, &nPriced); err != nil {
		log.Printf("finalizeIfReady total %s: %v", sku, err)
		return
	}

	status := "done"
	if nPriced < nItems {
		status = "partial" // some ASINs had no competitor data → counted as 0
	}
	recommended := round2(total.Float64 / 2)

	res, err := a.db.Exec(`
UPDATE pallets SET estimated_sale_price=?, recommended_bid=?, estimate_status=?, estimated_at=?, updated_at=?
WHERE sku=? AND estimate_status='in_progress'`,
		round2(total.Float64), recommended, status, nowISO(), nowISO(), sku)
	if err != nil {
		log.Printf("finalize update %s: %v", sku, err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("finalize %s: status=%s total=%.2f priced=%d/%d bid=%.2f", sku, status, round2(total.Float64), nPriced, nItems, recommended)
	}
}

func (a *App) finalizeAllReady() {
	rows, err := a.db.Query(`SELECT sku FROM pallets WHERE estimate_status='in_progress'`)
	if err != nil {
		log.Printf("finalizeAllReady: %v", err)
		return
	}
	var skus []string
	for rows.Next() {
		var sku string
		if err := rows.Scan(&sku); err == nil {
			skus = append(skus, sku)
		}
	}
	rows.Close()
	for _, sku := range skus {
		a.finalizeIfReady(sku)
	}
}

// sweepStale finalizes pallets stuck in_progress past the timeout (e.g. the
// competition module never called back for some ASIN). Unresolved items are
// forced to resolved with price 0, so the pallet finalizes as 'partial'.
func (a *App) sweepStale() {
	threshold := time.Now().UTC().Add(-a.cfg.EstimateTimeout).Format(time.RFC3339)
	rows, err := a.db.Query(`SELECT sku FROM pallets WHERE estimate_status='in_progress' AND estimated_at < ?`, threshold)
	if err != nil {
		log.Printf("sweepStale: %v", err)
		return
	}
	var skus []string
	for rows.Next() {
		var sku string
		if err := rows.Scan(&sku); err == nil {
			skus = append(skus, sku)
		}
	}
	rows.Close()

	for _, sku := range skus {
		if _, err := a.db.Exec(`UPDATE pallet_items SET resolved=1 WHERE sku=? AND resolved=0`, sku); err != nil {
			log.Printf("sweepStale resolve %s: %v", sku, err)
			continue
		}
		a.finalizeIfReady(sku)
		log.Printf("sweepStale: force-finalized stalled pallet %s", sku)
	}
}

func (a *App) setStatus(sku, status string) {
	if _, err := a.db.Exec(`UPDATE pallets SET estimate_status=?, estimated_at=?, updated_at=? WHERE sku=?`,
		status, nowISO(), nowISO(), sku); err != nil {
		log.Printf("setStatus %s=%s: %v", sku, status, err)
	}
}

func (a *App) setError(sku string, msg string) {
	if _, err := a.db.Exec(`UPDATE pallets SET estimate_status='error', error_message=?, estimated_at=?, updated_at=? WHERE sku=?`,
		msg, nowISO(), nowISO(), sku); err != nil {
		log.Printf("setError %s: %v", sku, err)
	}
}
