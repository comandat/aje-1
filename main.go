// estimator is the brain of the auction-automation system.
//
// It fetches tomorrow's jobalots pallet auctions, estimates each pallet's
// resale value and a recommended bid, and serves a dashboard + an API for the
// Chrome plugin. Product/competition data is reused from (and written back to)
// the central Postgres DB; the competition scraping is delegated to the
// existing competition-search-module (which calls us back asynchronously).
package main

import (
	"log"
	"os"
	"strconv"
	"time"
	_ "time/tzdata" // embed zoneinfo so Europe/Bucharest resolves on Windows too
)

// tzBucharest is the business timezone. "Tomorrow" means the next full calendar
// day in Bucharest (00:00–23:59), per spec.
var tzBucharest = loadTZ("Europe/Bucharest")

func loadTZ(name string) *time.Location {
	loc, err := time.LoadLocation(name)
	if err != nil {
		log.Printf("timezone %s unavailable (%v) — falling back to UTC", name, err)
		return time.UTC
	}
	return loc
}

// ourUserID is the jobalots user id of our account. When a pallet's current
// bid belongs to this id we are the highest bidder ("winning").
const defaultOurUserID = 59890

// defaultStateTree is the Next.js router-state-tree header value required by the
// jobalots products-on-auction server action. ponytail: copied verbatim from the
// proven n8n prototype; if jobalots redeploys their frontend this (and
// JOBALOTS_NEXT_ACTION) may change — both are env-overridable for that reason.
const defaultStateTree = "%5B%22%22%2C%7B%22children%22%3A%5B%5B%22lang%22%2C%22ro%22%2C%22d%22%5D%2C%7B%22children%22%3A%5B%22(website)%22%2C%7B%22children%22%3A%5B%22pages%22%2C%7B%22children%22%3A%5B%22products-on-auction%22%2C%7B%22children%22%3A%5B%22__PAGE__%22%2C%7B%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%5D%7D%2Cnull%2Cnull%2Ctrue%5D"

type Config struct {
	Port               string
	DBPath             string
	N8NLookupURL       string // n8n webhook: ASINs -> existing initial_estimated_price
	N8NSaveURL         string // n8n webhook: persist a product to the central DB
	ManifestToken      string
	CompetitionURL     string
	JobalotsNextAction string
	JobalotsStateTree  string
	OurUserID          int64
	DisableCron        bool
	EstimateTimeout    time.Duration
}

func loadConfig() Config {
	return Config{
		Port:               env("PORT", "8080"),
		DBPath:             env("DB_PATH", "data/auctions.db"),
		N8NLookupURL:       os.Getenv("N8N_LOOKUP_URL"),
		N8NSaveURL:         os.Getenv("N8N_SAVE_URL"),
		ManifestToken:      os.Getenv("JOBALOTS_MANIFEST_TOKEN"),
		CompetitionURL:     env("COMPETITION_MODULE_URL", "https://competition-search-module-production.up.railway.app/competition-search"),
		JobalotsNextAction: env("JOBALOTS_NEXT_ACTION", "7f85c3b96b7f0e4740df33c5cbb862e11a3d68028e"),
		JobalotsStateTree:  env("JOBALOTS_STATE_TREE", defaultStateTree),
		OurUserID:          int64(intEnv("OUR_USER_ID", defaultOurUserID)),
		DisableCron:        os.Getenv("DISABLE_CRON") == "1",
		EstimateTimeout:    time.Duration(intEnv("ESTIMATE_TIMEOUT_MIN", 15)) * time.Minute,
	}
}

func main() {
	log.SetOutput(os.Stdout)
	cfg := loadConfig()

	app, err := newApp(cfg)
	if err != nil {
		log.Fatalf("startup: %v", err)
	}
	defer app.Close()

	if !cfg.DisableCron {
		go app.runCron()
	}

	log.Printf("estimator listening on :%s (cron disabled=%v, estimation=%v)",
		cfg.Port, cfg.DisableCron, app.estimationEnabled())
	log.Printf("config: competition_url=%s", cfg.CompetitionURL)
	log.Printf("config: n8n_lookup=%s n8n_save=%s", cfg.N8NLookupURL, cfg.N8NSaveURL)
	log.Printf("config: manifest_token_set=%v", cfg.ManifestToken != "")
	if err := app.serve(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// runCron drives the background schedule. The Go service is self-contained: it
// refreshes auctions + current bids hourly, sweeps stalled estimations every
// couple of minutes, and purges expired pallets daily. The optional n8n
// workflows just hit the same endpoints for teams that prefer central control.
func (a *App) runCron() {
	a.fetchAndStore() // populate immediately so the dashboard isn't empty
	a.cleanupOld()

	hourly := time.NewTicker(time.Hour)
	sweep := time.NewTicker(2 * time.Minute)
	defer hourly.Stop()
	defer sweep.Stop()

	for {
		select {
		case <-hourly.C:
			a.fetchAndStore()
			a.cleanupOld()
		case <-sweep.C:
			a.sweepStale()
		}
	}
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func intEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
