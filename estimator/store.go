package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite"
)

// App holds the shared dependencies. SQLite is our own auction/estimate state;
// all central-Postgres access is delegated to n8n over HTTP (lookup + save).
type App struct {
	cfg  Config
	db   *sql.DB // sqlite, local state
	http *http.Client
}

func newApp(cfg Config) (*App, error) {
	if err := os.MkdirAll(filepath.Dir(cfg.DBPath), 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", cfg.DBPath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// ponytail: single writer connection serializes access — simplest correct
	// choice for SQLite. Bump + add retry only if write throughput ever matters.
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		return nil, err
	}

	if cfg.N8NLookupURL == "" || cfg.N8NSaveURL == "" {
		log.Print("WARNING: N8N_LOOKUP_URL / N8N_SAVE_URL not set — estimation is disabled (dashboard/fetch still work)")
	}

	return &App{
		cfg:  cfg,
		db:   db,
		http: &http.Client{Timeout: 45 * time.Second},
	}, nil
}

// estimationEnabled reports whether the n8n Postgres gateway is configured.
func (a *App) estimationEnabled() bool {
	return a.cfg.N8NLookupURL != "" && a.cfg.N8NSaveURL != ""
}

func (a *App) Close() {
	if a.db != nil {
		a.db.Close()
	}
}

func migrate(db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS pallets (
  sku                  TEXT PRIMARY KEY,
  manifest_sku         TEXT,
  title                TEXT,
  country              TEXT,
  end_at               TEXT,
  start_at             TEXT,
  rrp                  REAL,
  weight               REAL,
  qty                  INTEGER,
  bid_count            INTEGER,
  current_bid          REAL,
  current_bid_user_id  INTEGER,
  currency             TEXT,
  auction_url          TEXT,
  image                TEXT DEFAULT '',
  condition            TEXT DEFAULT '',
  estimated_sale_price REAL,
  recommended_bid      REAL,
  estimate_status      TEXT DEFAULT '',
  error_message        TEXT DEFAULT '',
  estimated_at         TEXT,
  updated_at           TEXT
);
CREATE TABLE IF NOT EXISTS pallet_items (
  sku          TEXT,
  asin         TEXT,
  qty          INTEGER DEFAULT 1,
  image        TEXT,
  title        TEXT,
  description  TEXT,
  images       TEXT,
  brand        TEXT,
  ean          TEXT,
  category     TEXT,
  subcategory  TEXT,
  unit_weight  TEXT,
  price        REAL DEFAULT 0,
  resolved     INTEGER DEFAULT 0,
  requested    INTEGER DEFAULT 0,
  PRIMARY KEY (sku, asin)
);
CREATE INDEX IF NOT EXISTS idx_items_asin ON pallet_items(asin);
`
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	// Forward-compatible: add columns missing in DBs created before this version.
	existing := map[string]bool{}
	if rows, err := db.Query("PRAGMA table_info(pallets)"); err == nil {
		var cid, notnull, pk int
		var name, typ string
		var dflt sql.NullString
		for rows.Next() {
			if rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk) == nil {
				existing[name] = true
			}
		}
		rows.Close()
	}
	for col, stmt := range map[string]string{
		"image":         `ALTER TABLE pallets ADD COLUMN image TEXT DEFAULT ''`,
		"condition":     `ALTER TABLE pallets ADD COLUMN condition TEXT DEFAULT ''`,
		"error_message": `ALTER TABLE pallets ADD COLUMN error_message TEXT DEFAULT ''`,
	} {
		if !existing[col] {
			db.Exec(stmt)
		}
	}
	return nil
}

// upsertPallet inserts/updates auction data without ever touching the estimate
// columns (those are owned by the estimation pipeline).
func (a *App) upsertPallet(p Pallet) error {
	const q = `
INSERT INTO pallets (sku, manifest_sku, title, country, end_at, start_at, rrp, weight, qty,
  bid_count, current_bid, current_bid_user_id, currency, auction_url, image, condition, estimate_status, updated_at)
VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'',?)
ON CONFLICT(sku) DO UPDATE SET
  manifest_sku=excluded.manifest_sku, title=excluded.title, country=excluded.country,
  end_at=excluded.end_at, start_at=excluded.start_at, rrp=excluded.rrp, weight=excluded.weight,
  qty=excluded.qty, bid_count=excluded.bid_count, current_bid=excluded.current_bid,
  current_bid_user_id=excluded.current_bid_user_id, currency=excluded.currency,
  auction_url=excluded.auction_url, image=excluded.image, condition=excluded.condition,
  updated_at=excluded.updated_at;`
	_, err := a.db.Exec(q, p.SKU, p.ManifestSKU, p.Title, p.Country, p.EndAt, p.StartAt,
		p.RRP, p.Weight, p.Qty, p.BidCount, p.CurrentBid, p.CurrentBidUserID, p.Currency,
		p.AuctionURL, p.Image, p.Condition, nowISO())
	return err
}

func (a *App) getPallet(sku string) (*Pallet, error) {
	const q = `
SELECT sku, manifest_sku, title, country, end_at, start_at, rrp, weight, qty, bid_count,
  current_bid, current_bid_user_id, currency, auction_url, image, condition,
  estimated_sale_price, recommended_bid, estimate_status, error_message, estimated_at,
  (SELECT COUNT(*) FROM pallet_items i WHERE i.sku=p.sku),
  (SELECT COUNT(*) FROM pallet_items i WHERE i.sku=p.sku AND i.resolved=1)
FROM pallets p WHERE sku=?;`
	return scanPallet(a.db.QueryRow(q, sku), a.cfg.OurUserID)
}

func (a *App) listPallets() ([]Pallet, error) {
	const q = `
SELECT sku, manifest_sku, title, country, end_at, start_at, rrp, weight, qty, bid_count,
  current_bid, current_bid_user_id, currency, auction_url, image, condition,
  estimated_sale_price, recommended_bid, estimate_status, error_message, estimated_at,
  (SELECT COUNT(*) FROM pallet_items i WHERE i.sku=p.sku),
  (SELECT COUNT(*) FROM pallet_items i WHERE i.sku=p.sku AND i.resolved=1)
FROM pallets p ORDER BY end_at ASC;`
	rows, err := a.db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Pallet
	for rows.Next() {
		p, err := scanPallet(rows, a.cfg.OurUserID)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// cleanupOld removes pallets that ended before the start of *yesterday*
// (Bucharest) — i.e. it keeps yesterday, today and tomorrow, so the day after
// tomorrow today's auctions are gone. end_at is a fixed-width ISO8601 UTC string
// so lexicographic comparison against an identically-formatted threshold holds.
func (a *App) cleanupOld() {
	y := time.Now().In(tzBucharest).AddDate(0, 0, -1)
	startOfYesterday := time.Date(y.Year(), y.Month(), y.Day(), 0, 0, 0, 0, tzBucharest)
	threshold := startOfYesterday.UTC().Format("2006-01-02T15:04:05.000000Z")
	if _, err := a.db.Exec(`DELETE FROM pallet_items WHERE sku IN (SELECT sku FROM pallets WHERE end_at < ?)`, threshold); err != nil {
		log.Printf("cleanup items: %v", err)
	}
	res, err := a.db.Exec(`DELETE FROM pallets WHERE end_at < ?`, threshold)
	if err != nil {
		log.Printf("cleanup pallets: %v", err)
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Printf("cleanup: removed %d expired pallets", n)
	}
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanPallet(row rowScanner, ourUserID int64) (*Pallet, error) {
	var p Pallet
	var estSale, recBid sql.NullFloat64
	var estimatedAt sql.NullString
	err := row.Scan(&p.SKU, &p.ManifestSKU, &p.Title, &p.Country, &p.EndAt, &p.StartAt,
		&p.RRP, &p.Weight, &p.Qty, &p.BidCount, &p.CurrentBid, &p.CurrentBidUserID,
		&p.Currency, &p.AuctionURL, &p.Image, &p.Condition,
		&estSale, &recBid, &p.EstimateStatus, &p.ErrorMessage, &estimatedAt,
		&p.ItemsCount, &p.ResolvedCount)
	if err != nil {
		return nil, err
	}
	if estSale.Valid {
		p.EstimatedSalePrice = &estSale.Float64
	}
	if recBid.Valid {
		p.RecommendedBid = &recBid.Float64
	}
	if estimatedAt.Valid {
		p.EstimatedAt = &estimatedAt.String
	}
	p.Winning = p.CurrentBidUserID == ourUserID && p.CurrentBidUserID != 0
	return &p, nil
}

func nowISO() string {
	return time.Now().UTC().Format(time.RFC3339)
}
