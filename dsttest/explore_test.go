package dsttest_test

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// twoRare fails only when a rare event A (1 in 100 runs) happens and,
// later, a second event (1 in 10) follows it: a sweep needs about 1,000
// seeds per failure; forking from where A happened, about 10 runs.
func twoRare(h *dsttest.Harness) {
	h.Go(func(w *dst.World) {
		a := false
		for round := range 120 {
			switch round {
			case 10:
				h.Sometimes("started")
				if w.Rand().IntN(100) == 0 {
					a = true
					h.Sometimes("A happened")
				}
			case 100:
				if a && w.Rand().IntN(10) == 0 {
					h.Fail(errors.New("A, then B"))
				}
			}
			w.Rand().Uint64() // the run's other randomness
			w.Sleep(time.Millisecond)
		}
	})
}

var exploreOpts = dsttest.Options{FaultPhase: 200 * time.Millisecond, RecoveryPhase: 10 * time.Millisecond, DeterminismChecks: -1}

// Explore forks from a rare marker and finds a bug that needs a second
// event after it several times more often than a sweep of as many runs
// (expected: about 6 failures against 0.6). Fails if it doesn't.
func TestExploreFindsWhatASweepMisses(t *testing.T) {
	o := exploreOpts
	o.Seeds, o.Explore, o.KeepGoing = 600, 600, true
	ex := dsttest.Explore(o, twoRare)
	sw := dsttest.Sweep(o, twoRare)
	if len(ex.Failed) < 3 || len(ex.Failed) < 3*len(sw.Failed) {
		t.Fatalf("exploration failed %d runs, the sweep %d: want several times more", len(ex.Failed), len(sw.Failed))
	}
	t.Logf("exploration: %d failing runs; sweep: %d", len(ex.Failed), len(sw.Failed))
	f := ex.Failed[0]
	if len(f.Path.Forks) == 0 || f.Path.Forks[len(f.Path.Forks)-1].Marker != "A happened" {
		t.Errorf("the failure %v wasn't a fork from the rare marker", f.Path)
	}
	// Its failing path replays on its own.
	if r := dsttest.RunPath(f.Path, o, twoRare, nil); r.Err == nil {
		t.Errorf("path %v passed on replay", f.Path)
	}
}

// An exploration is the same whatever the parallelism: the same paths, in
// the same order. Fails if they differ.
func TestExploreDeterministic(t *testing.T) {
	run := func(par int) []string {
		o := exploreOpts
		o.Explore, o.Parallel, o.KeepGoing = 200, par, true
		var out []string
		for _, r := range dsttest.Explore(o, twoRare).Results {
			out = append(out, r.Path.String())
		}
		return out
	}
	a, b := run(1), run(8)
	if !slices.Equal(a, b) {
		t.Fatalf("explorations differ with 1 and 8 at once")
	}
	forked := 0
	for _, p := range a {
		if len(p) > 0 && slices.Contains([]byte(p), '/') {
			forked++
		}
	}
	// Two markers, each forked at most ExploreBudget (20) times.
	if forked != 40 {
		t.Errorf("%d of %d runs were forks, want 40 (two markers' budgets)", forked, len(a))
	}
}
