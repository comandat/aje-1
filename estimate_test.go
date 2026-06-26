package main

import (
	"encoding/json"
	"testing"
)

func TestPriceFromProducts(t *testing.T) {
	// avg(100,200,300)=200 -> 200*.8*.75*.95 = 114.00
	got := priceFromProducts([]compProduct{
		{CurrentPrice: 100}, {CurrentPrice: 200}, {CurrentPrice: 300}, {CurrentPrice: 999},
	})
	if got != 114.0 {
		t.Fatalf("3 prices: want 114.0, got %v", got)
	}

	// zero-priced entries are skipped; still only the first 3 valid are used
	got = priceFromProducts([]compProduct{
		{CurrentPrice: 0}, {CurrentPrice: 100}, {CurrentPrice: 200}, {CurrentPrice: 300}, {CurrentPrice: 400},
	})
	if got != 114.0 {
		t.Fatalf("skip-zero: want 114.0, got %v", got)
	}

	if got := priceFromProducts(nil); got != 0 {
		t.Fatalf("no products: want 0, got %v", got)
	}
}

func TestParseKnownPrices(t *testing.T) {
	cases := map[string]map[string]float64{
		`[{"asin":"b01","price":"12.5"},{"asin":"B02","price":3}]`: {"B01": 12.5, "B02": 3},
		`{"asin":"b03","price":7.25}`:                              {"B03": 7.25},
		`{"data":[{"asin":"b04","price":"9"}]}`:                    {"B04": 9},
		``:                                                         {},
		`[]`:                                                       {},
	}
	for in, want := range cases {
		got, err := parseKnownPrices([]byte(in))
		if err != nil {
			t.Fatalf("parseKnownPrices(%q): %v", in, err)
		}
		if len(got) != len(want) {
			t.Fatalf("parseKnownPrices(%q): want %v, got %v", in, want, got)
		}
		for k, v := range want {
			if got[k] != v {
				t.Fatalf("parseKnownPrices(%q)[%s]: want %v, got %v", in, k, v, got[k])
			}
		}
	}
}

func TestDeriveManifestSKU(t *testing.T) {
	cases := map[string]string{
		"YELLOW3262520260618": "YELLOW32625",
		"ABC":                 "ABC",
		"GREEN0000120251231":  "GREEN00001",
	}
	for in, want := range cases {
		if got := deriveManifestSKU(in); got != want {
			t.Fatalf("deriveManifestSKU(%q): want %q, got %q", in, want, got)
		}
	}
}

func TestFlexFloat(t *testing.T) {
	cases := map[string]float64{
		`"7895.98"`: 7895.98,
		`336`:       336,
		`""`:        0,
		`null`:      0,
		`"abc"`:     0,
	}
	for in, want := range cases {
		var f flexFloat
		if err := json.Unmarshal([]byte(in), &f); err != nil {
			t.Fatalf("unmarshal %s: %v", in, err)
		}
		if float64(f) != want {
			t.Fatalf("flexFloat(%s): want %v, got %v", in, want, float64(f))
		}
	}
}
