package dsttest

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
)

// A KeepGoing sweep reports every seed in Results, in seed order, and
// -dst.json writes one line per seed with its position and outcome. Fails
// if a seed is missing, out of order, or its failure is lost.
func TestSweepResultsAndJSON(t *testing.T) {
	opts := Options{Seeds: 12, FirstSeed: 40, KeepGoing: true, FaultPhase: 10_000_000, RecoveryPhase: 10_000_000}
	rep := Sweep(opts, func(h *Harness) {
		h.Machine("m", func(w *dst.World) {
			h.Sometimes("ran")
			if h.Seed()%3 == 0 {
				h.Fail(errors.New("multiple of three"))
			}
		})
	})
	if len(rep.Results) != 12 {
		t.Fatalf("%d results, want 12", len(rep.Results))
	}
	path := filepath.Join(t.TempDir(), "seeds.jsonl")
	if err := writeJSON(path, "TestX", rep); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	n := 0
	for sc.Scan() {
		var rec seedRecord
		if err := json.Unmarshal(sc.Bytes(), &rec); err != nil {
			t.Fatal(err)
		}
		if rec.Seed != 40+uint64(n) || rec.Index != n || rec.Test != "TestX" {
			t.Fatalf("line %d: seed %d index %d test %q", n, rec.Seed, rec.Index, rec.Test)
		}
		if want := rec.Seed%3 == 0; rec.Failed != want || (want && rec.Error == "") {
			t.Fatalf("seed %d: failed %v (%q), want %v", rec.Seed, rec.Failed, rec.Error, want)
		}
		if rec.Coverage["ran"] != 1 || rec.WallNS <= 0 {
			t.Fatalf("seed %d: coverage %v, wall %d", rec.Seed, rec.Coverage, rec.WallNS)
		}
		n++
	}
	if n != 12 {
		t.Fatalf("%d lines, want 12", n)
	}
}
