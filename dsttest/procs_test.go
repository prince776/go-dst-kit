package dsttest

import (
	"fmt"
	"hash/fnv"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Verifies: [procs-drawn-per-seed]
// Fails if: a simulated World's GOMAXPROCS or NumCPU isn't its seed's
// draw (Swarm), isn't one of 1, 2, 4, 8 (NumCPU at least it), differs
// between machines or between a run and its replay, follows the
// process's own count, misses one of the four values across seeds, or
// isn't in the seed's report; or if drawing it changed any other draw
// of the swarm (a seed must mean what it meant before).
func TestProcsDrawnPerSeed(t *testing.T) {
	seen := map[int]bool{}
	setup := func(got map[string][2]int) func(h *Harness) {
		return func(h *Harness) {
			for _, name := range []string{"a", "b"} {
				h.Machine(name, func(w *dst.World) {
					got[name] = [2]int{w.GOMAXPROCS(), w.NumCPU()}
				})
			}
		}
	}
	opts := Options{Seeds: 1, FaultPhase: time.Second, RecoveryPhase: time.Second}
	for seed := uint64(1); seed <= 40; seed++ {
		got := map[string][2]int{}
		r := RunSeed(seed, opts, setup(got), nil)
		if r.Err != nil {
			t.Fatalf("seed %d: %v", seed, r.Err)
		}
		p, c := r.Swarm.GOMAXPROCS, r.Swarm.NumCPU
		if (p != 1 && p != 2 && p != 4 && p != 8) || c < p {
			t.Errorf("seed %d drew GOMAXPROCS %d, NumCPU %d", seed, p, c)
		}
		for name, g := range got {
			if g != [2]int{p, c} {
				t.Errorf("seed %d: machine %s reports %v, the seed drew %d/%d", seed, name, g, p, c)
			}
		}
		if !strings.Contains(r.Swarm.String(), fmt.Sprintf("GOMAXPROCS %d, NumCPU %d", p, c)) {
			t.Errorf("seed %d: the report lacks the draw: %s", seed, r.Swarm)
		}
		again := map[string][2]int{}
		if RunSeed(seed, opts, setup(again), nil); fmt.Sprint(again) != fmt.Sprint(got) {
			t.Errorf("seed %d: replay reports %v, first run %v", seed, again, got)
		}
		seen[p] = true
	}
	if len(seen) != 4 {
		t.Errorf("GOMAXPROCS values across 40 seeds: %v, want all of 1, 2, 4, 8", seen)
	}
	_ = runtime.GOMAXPROCS // the process's own count (2 here) is one value; all four appear

	// Every other draw is what it was before GOMAXPROCS and NumCPU were
	// added (a fingerprint taken then).
	type plain Swarm // every field, not String's
	h := fnv.New64a()
	for seed := uint64(1); seed <= 200; seed++ {
		s := newSwarm(seed, AllFaults, 10*time.Second)
		s.GOMAXPROCS, s.NumCPU = 0, 0
		fmt.Fprintf(h, "%s\n", strings.Replace(fmt.Sprintf("%+v", plain(s)), " GOMAXPROCS:0 NumCPU:0}", "}", 1))
	}
	if got := h.Sum64(); got != 0xd4c3661158cb7cc0 {
		t.Errorf("the swarm's other draws changed: fingerprint %#x", got)
	}
}
