package main

import (
	_ "embed"
	"encoding/json"
	"log"
	"net/http"
)

//go:embed web/index.html
var indexHTML []byte

func (a *App) serve() error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{$}", a.handleIndex)
	mux.HandleFunc("GET /api/pallets", a.handleListPallets)
	mux.HandleFunc("POST /api/estimate", a.handleEstimateAll)
	mux.HandleFunc("POST /api/estimate/{sku}", a.handleEstimateOne)
	mux.HandleFunc("GET /api/pallet/{sku}", a.handlePalletOne)
	mux.HandleFunc("POST /api/competition-callback", a.handleCallback)
	mux.HandleFunc("POST /api/refresh", a.handleRefresh)
	mux.HandleFunc("POST /api/refresh-bids", a.handleRefresh)
	return http.ListenAndServe(":"+a.cfg.Port, cors(mux))
}

// cors allows the Chrome extension (running on jobalots.com) to call the API.
func cors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (a *App) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(indexHTML)
}

func (a *App) handleListPallets(w http.ResponseWriter, r *http.Request) {
	pallets, err := a.listPallets()
	if err != nil {
		httpError(w, err)
		return
	}
	if pallets == nil {
		pallets = []Pallet{}
	}
	writeJSON(w, pallets)
}

func (a *App) handleEstimateAll(w http.ResponseWriter, r *http.Request) {
	if !a.estimationEnabled() {
		http.Error(w, "n8n postgres gateway not configured", http.StatusServiceUnavailable)
		return
	}
	n := a.startEstimateAll()
	writeJSON(w, map[string]int{"launched": n})
}

// handlePalletOne backs the Chrome plugin. Returns the current estimate state;
// never auto-starts estimation (user triggers that from the dashboard).
func (a *App) handlePalletOne(w http.ResponseWriter, r *http.Request) {
	sku := r.PathValue("sku")
	p, err := a.getPallet(sku)
	if err != nil {
		httpError(w, err)
		return
	}
	if p == nil {
		writeJSON(w, map[string]string{"estimate_status": "not_found"})
		return
	}
	writeJSON(w, p)
}

func (a *App) handleEstimateOne(w http.ResponseWriter, r *http.Request) {
	if !a.estimationEnabled() {
		http.Error(w, "n8n postgres gateway not configured", http.StatusServiceUnavailable)
		return
	}
	go a.estimatePallet(r.PathValue("sku"))
	writeJSON(w, map[string]bool{"ok": true})
}

func (a *App) handleCallback(w http.ResponseWriter, r *http.Request) {
	var results []CompetitionResult
	if err := json.NewDecoder(r.Body).Decode(&results); err != nil {
		http.Error(w, "invalid payload", http.StatusBadRequest)
		return
	}
	log.Printf("competition callback: %d results", len(results))
	go a.onCompetitionCallback(results) // ack fast; process in background
	writeJSON(w, map[string]any{"ok": true, "received": len(results)})
}

func (a *App) handleRefresh(w http.ResponseWriter, r *http.Request) {
	go func() {
		a.fetchAndStore()
		a.cleanupOld()
	}()
	writeJSON(w, map[string]bool{"ok": true})
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, err error) {
	log.Printf("http error: %v", err)
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
