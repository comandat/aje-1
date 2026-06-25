package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/xuri/excelize/v2"
)

// Pallet is both our SQLite row and the dashboard/plugin API shape.
type Pallet struct {
	SKU                string   `json:"sku"`
	ManifestSKU        string   `json:"manifest_sku"`
	Title              string   `json:"title"`
	Country            string   `json:"country"`
	EndAt              string   `json:"end_at"`
	StartAt            string   `json:"start_at"`
	RRP                float64  `json:"rrp"`
	Weight             float64  `json:"weight"`
	Qty                int      `json:"qty"`
	BidCount           int      `json:"bid_count"`
	CurrentBid         float64  `json:"current_bid"`
	CurrentBidUserID   int64    `json:"current_bid_user_id"`
	Currency           string   `json:"currency"`
	AuctionURL         string   `json:"auction_url"`
	Image              string   `json:"image"`
	Condition          string   `json:"condition"`
	EstimatedSalePrice *float64 `json:"estimated_sale_price"`
	RecommendedBid     *float64 `json:"recommended_bid"`
	EstimateStatus     string   `json:"estimate_status"`
	EstimatedAt        *string  `json:"estimated_at"`
	Winning            bool     `json:"winning"`
	ItemsCount         int      `json:"items_count"`
	ResolvedCount      int      `json:"resolved_count"`
}

// ManifestItem is one (aggregated-by-ASIN) line of a pallet manifest.
type ManifestItem struct {
	ASIN        string
	Qty         int
	Image       string
	Title       string
	Description string
	Images      []string
	Brand       string
	EAN         string
	Category    string
	Subcategory string
	UnitWeight  string
}

// flexFloat parses a number that jobalots may send as either a JSON number
// (336) or a quoted string ("7895.98" / "" / null). Lenient: bad input -> 0.
type flexFloat float64

func (f *flexFloat) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	if s == "" || s == "null" {
		*f = 0
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		*f = 0
		return nil
	}
	*f = flexFloat(v)
	return nil
}

type rawPallet struct {
	SKU            string    `json:"sku"`
	Title          string    `json:"title"`
	CountryISO     string    `json:"country_iso"`
	EndAt          string    `json:"end_at"`
	StartAt        string    `json:"start_at"`
	RRP            flexFloat `json:"rrp"`
	Weight         flexFloat `json:"weight"`
	Qty            int       `json:"qty"`
	BidCount       int       `json:"bid_count"`
	LatestBidPrice flexFloat `json:"latest_bid_price"`
	Currency       string    `json:"currency"`
	AuctionURL     string    `json:"auction_url"`
	CurrentBid     struct {
		BidPrice flexFloat `json:"bid_price"`
		UserID   int64     `json:"user_id"`
	} `json:"current_bid"`
	Manifest struct {
		ProductFirstImage struct {
			ThumbnailURL string `json:"product_image_thumbnail_url"`
		} `json:"product_first_image"`
		ConditionNames []string `json:"condition_names"`
	} `json:"manifest"`
}

func (rp rawPallet) toPallet() Pallet {
	bid := float64(rp.LatestBidPrice)
	if bid == 0 {
		bid = float64(rp.CurrentBid.BidPrice)
	}
	condition := ""
	if len(rp.Manifest.ConditionNames) > 0 {
		condition = rp.Manifest.ConditionNames[0]
	}
	return Pallet{
		SKU:              rp.SKU,
		ManifestSKU:      deriveManifestSKU(rp.SKU),
		Title:            rp.Title,
		Country:          rp.CountryISO,
		EndAt:            rp.EndAt,
		StartAt:          rp.StartAt,
		RRP:              float64(rp.RRP),
		Weight:           float64(rp.Weight),
		Qty:              rp.Qty,
		BidCount:         rp.BidCount,
		CurrentBid:       bid,
		CurrentBidUserID: rp.CurrentBid.UserID,
		Currency:         rp.Currency,
		AuctionURL:       rp.AuctionURL,
		Image:            rp.Manifest.ProductFirstImage.ThumbnailURL,
		Condition:        condition,
	}
}

type auctionEnvelope struct {
	Result struct {
		Data        []rawPallet `json:"data"`
		CurrentPage int         `json:"current_page"`
		LastPage    int         `json:"last_page"`
	} `json:"result"`
}

var (
	// RSC stream line that carries the auction-list JSON: `1:{...}`.
	rscLine = regexp.MustCompile(`(?m)^1:(\{.*\})\s*$`)
	// Fallback used by the original n8n prototype (bounded to the total field).
	rscFallback = regexp.MustCompile(`(?s)1:(\{"error":false,.*?"total":\d+\}\})`)
	// Manifest SKU = pallet SKU minus the trailing YYYYMMDD (e.g. YELLOW3262520260618 -> YELLOW32625).
	skuDateSuffix = regexp.MustCompile(`^(.*?)(\d{8})$`)
)

func deriveManifestSKU(sku string) string {
	if m := skuDateSuffix.FindStringSubmatch(sku); m != nil {
		return m[1]
	}
	return sku
}

func (a *App) fetchAndStore() {
	pallets, err := a.fetchTomorrowPallets()
	if err != nil {
		log.Printf("fetch auctions: %v", err)
		// Keep whatever pages we did collect — partial is better than nothing.
	}
	for _, p := range pallets {
		if err := a.upsertPallet(p); err != nil {
			log.Printf("upsert pallet %s: %v", p.SKU, err)
		}
	}
	log.Printf("fetch: stored %d pallets for tomorrow", len(pallets))
}

// fetchTomorrowPallets walks the auction list (sorted end-soonest-first) and
// collects every pallet ending tomorrow in Bucharest local time. It stops once
// the list passes tomorrow or runs out of pages.
func (a *App) fetchTomorrowPallets() ([]Pallet, error) {
	tomorrow := time.Now().In(tzBucharest).AddDate(0, 0, 1).Format("2006-01-02")
	var collected []Pallet
	seen := map[string]bool{}

	for page := 1; page <= 60; page++ { // ponytail: 60-page hard cap (>6000 pallets) as a runaway guard.
		env, err := a.fetchAuctionPage(page)
		if err != nil {
			return collected, err
		}

		for _, rp := range env.Result.Data {
			if bucharestDate(rp.EndAt) == tomorrow && !seen[rp.SKU] {
				seen[rp.SKU] = true
				collected = append(collected, rp.toPallet())
			}
		}

		stop := env.Result.CurrentPage >= env.Result.LastPage
		if n := len(env.Result.Data); n == 0 {
			stop = true
		} else if last := bucharestDate(env.Result.Data[n-1].EndAt); last != "" && last > tomorrow {
			stop = true
		}
		if stop {
			break
		}
	}
	return collected, nil
}

// bucharestDate parses a jobalots end_at (RFC3339 UTC) and returns its calendar
// date in Bucharest local time ("2006-01-02"), or "" if unparseable.
func bucharestDate(endAt string) string {
	t, err := time.Parse(time.RFC3339, endAt)
	if err != nil {
		return ""
	}
	return t.In(tzBucharest).Format("2006-01-02")
}

func (a *App) fetchAuctionPage(page int) (auctionEnvelope, error) {
	var env auctionEnvelope

	body := fmt.Sprintf(`[{"url":"/auction-list-v2","method":"POST","headers":{"url-accept-language":"en","url-accept-currency":"ron"},"body":{"per_page":100,"page":%d,"use_open_search":"1","exact_match":"0","search_manifests":"0","sort_by":"auction_end_soon","manifest_type":["pallets"],"ship_to":"ro","ship_from":"all","list_type":["auction","buyitnow"],"is_list":true},"timeout":600000}]`, page)
	u := fmt.Sprintf("https://jobalots.com/ro/pages/products-on-auction?page=%d&type=pallets&currency=ron", page)

	req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(body))
	if err != nil {
		return env, err
	}
	req.Header.Set("accept", "text/x-component")
	req.Header.Set("content-type", "text/plain;charset=UTF-8")
	req.Header.Set("next-action", a.cfg.JobalotsNextAction)
	req.Header.Set("next-router-state-tree", a.cfg.JobalotsStateTree)
	req.Header.Set("accept-language", "ro-RO,ro;q=0.9,en-US;q=0.8")
	req.Header.Set("accept-encoding", "identity")
	req.Header.Set("user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

	resp, err := a.http.Do(req)
	if err != nil {
		return env, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return env, err
	}
	if resp.StatusCode != http.StatusOK {
		return env, fmt.Errorf("auction list page %d: status %d", page, resp.StatusCode)
	}

	m := rscLine.FindSubmatch(data)
	if m == nil {
		m = rscFallback.FindSubmatch(data)
	}
	if m == nil {
		return env, fmt.Errorf("auction list page %d: could not locate data block", page)
	}
	if err := json.Unmarshal(m[1], &env); err != nil {
		return env, fmt.Errorf("auction list page %d: parse: %w", page, err)
	}
	return env, nil
}

// downloadManifest resolves a manifest SKU to its product spreadsheet, then
// parses the spreadsheet into aggregated-by-ASIN items.
func (a *App) downloadManifest(manifestSKU string) ([]ManifestItem, error) {
	if a.cfg.ManifestToken == "" {
		return nil, fmt.Errorf("JOBALOTS_MANIFEST_TOKEN not configured")
	}

	reqBody, _ := json.Marshal(map[string]string{"manifest_spw": manifestSKU})
	req, err := http.NewRequest(http.MethodPost, "https://live.jobalots.com/api/download-manifest", bytes.NewReader(reqBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("authorization", "Bearer "+a.cfg.ManifestToken)
	req.Header.Set("content-type", "application/json")
	req.Header.Set("accept", "application/json")
	req.Header.Set("accept-currency", "ron")
	req.Header.Set("accept-language", "en")
	req.Header.Set("url-accept-currency", "ron")
	req.Header.Set("url-accept-language", "en")

	resp, err := a.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("download-manifest %s: status %d", manifestSKU, resp.StatusCode)
	}

	var dm struct {
		Result struct {
			ManifestURL string `json:"manifest_url"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&dm); err != nil {
		return nil, fmt.Errorf("download-manifest %s: parse: %w", manifestSKU, err)
	}
	if dm.Result.ManifestURL == "" {
		return nil, fmt.Errorf("download-manifest %s: empty manifest_url", manifestSKU)
	}

	return a.parseManifestXLSX(dm.Result.ManifestURL)
}

func (a *App) parseManifestXLSX(url string) ([]ManifestItem, error) {
	resp, err := a.http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest xlsx: status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	f, err := excelize.OpenReader(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("open xlsx: %w", err)
	}
	defer f.Close()

	rows, err := f.GetRows(f.GetSheetName(0))
	if err != nil {
		return nil, fmt.Errorf("read xlsx: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("manifest xlsx: no data rows")
	}

	col := map[string]int{}
	for i, name := range rows[0] {
		col[strings.TrimSpace(name)] = i
	}
	get := func(row []string, name string) string {
		if i, ok := col[name]; ok && i < len(row) {
			v := strings.TrimSpace(row[i])
			if v == "N/A" {
				return ""
			}
			return v
		}
		return ""
	}

	byASIN := map[string]*ManifestItem{}
	var order []string
	for _, row := range rows[1:] {
		asin := strings.ToUpper(get(row, "ASIN"))
		if asin == "" {
			continue
		}

		qty, err := strconv.Atoi(get(row, "Quantity"))
		if err != nil || qty <= 0 {
			qty = 1 // a manifest line is at least one unit
		}

		if existing, ok := byASIN[asin]; ok {
			existing.Qty += qty
			continue
		}

		var images []string
		for n := 1; n <= 6; n++ {
			if img := get(row, fmt.Sprintf("Image %d", n)); img != "" {
				images = append(images, img)
			}
		}
		image := ""
		if len(images) > 0 {
			image = images[0]
		}

		byASIN[asin] = &ManifestItem{
			ASIN:        asin,
			Qty:         qty,
			Image:       image,
			Title:       get(row, "Product Title"),
			Description: get(row, "Product Description"),
			Images:      images,
			Brand:       get(row, "Brand"),
			EAN:         get(row, "EAN"), // ponytail: kept raw; Excel may mangle long EANs to sci-notation — source-side fix.
			Category:    get(row, "Category Name"),
			Subcategory: get(row, "Sub Category Name"),
			UnitWeight:  get(row, "Unit Weight (kg)"),
		}
		order = append(order, asin)
	}

	items := make([]ManifestItem, 0, len(order))
	for _, asin := range order {
		items = append(items, *byASIN[asin])
	}
	return items, nil
}
