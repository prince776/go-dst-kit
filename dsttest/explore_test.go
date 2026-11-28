package dsttest_test

import (
	"errors"
	"slices"
	"sync"
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

// twoRareUnmarked is twoRare with no markers: the rare event only changes
// where the goroutine waits (another line, so other interleaving pairs).
func twoRareUnmarked(h *dsttest.Harness) {
	h.Go(func(w *dst.World) {
		a := false
		for round := range 120 {
			switch round {
			case 10:
				a = w.Rand().IntN(100) == 0
			case 100:
				if a && w.Rand().IntN(10) == 0 {
					h.Fail(errors.New("A, then B"))
				}
			}
			w.Rand().Uint64()
			if a {
				w.Sleep(time.Millisecond) // after A: a site no run without A has
			} else {
				w.Sleep(time.Millisecond)
			}
		}
	})
}

var exploreOpts = dsttest.Options{FaultPhase: 200 * time.Millisecond, RecoveryPhase: 10 * time.Millisecond, DeterminismChecks: -1}

// Explore forks from a rare marker and finds a bug that needs a second
// event after it several times more often than a sweep of as many runs
// (expected: about 6 failures against 0.6), by markers alone and by every
// signal. Fails if it doesn't, or if, by markers, the first failure isn't
// a fork from the rare marker, or if a failing path doesn't replay.
func TestExploreFindsWhatASweepMisses(t *testing.T) {
	o := exploreOpts
	o.Seeds, o.Explore, o.KeepGoing = 600, 600, true
	sw := dsttest.Sweep(o, twoRare)
	for _, by := range []string{"markers", ""} {
		o.ExploreBy = by
		ex := dsttest.Explore(o, twoRare)
		if len(ex.Failed) < 3 || len(ex.Failed) < 3*len(sw.Failed) {
			t.Fatalf("exploration by %q failed %d runs, the sweep %d: want several times more", by, len(ex.Failed), len(sw.Failed))
		}
		t.Logf("exploration by %q: %d failing runs; sweep: %d", by, len(ex.Failed), len(sw.Failed))
		f := ex.Failed[0]
		if len(f.Path.Forks) == 0 {
			t.Errorf("by %q: the first failure %v isn't a fork", by, f.Path)
		}
		if by == "markers" && f.Path.Forks[len(f.Path.Forks)-1].Marker != "A happened" {
			t.Errorf("the failure %v wasn't a fork from the rare marker", f.Path)
		}
		// Its failing path replays on its own.
		if r := dsttest.RunPath(f.Path, o, twoRare, nil); r.Err == nil {
			t.Errorf("by %q: path %v passed on replay", by, f.Path)
		}
	}
}

// With no marker to steer by, interleaving pairs still find the rare
// state (it waits at a line other runs don't) and the bug after it several
// times more often than a sweep, and than exploring by markers (which,
// with none, is a sweep): expected about 10 failing runs against 1. Fails
// if they don't, or if a failing path doesn't replay.
func TestExploreByPairsNeedsNoMarkers(t *testing.T) {
	o := exploreOpts
	o.Seeds, o.Explore, o.KeepGoing = 1000, 1000, true
	sw := dsttest.Sweep(o, twoRareUnmarked)
	o.ExploreBy = "markers"
	byMarkers := dsttest.Explore(o, twoRareUnmarked)
	o.ExploreBy = "pairs"
	byPairs := dsttest.Explore(o, twoRareUnmarked)
	t.Logf("failing runs: by pairs %d, by markers %d, sweep %d", len(byPairs.Failed), len(byMarkers.Failed), len(sw.Failed))
	if n := len(byPairs.Failed); n < 3 || n < 3*len(sw.Failed) || n < 3*len(byMarkers.Failed) {
		t.Fatalf("by pairs %d failing runs, by markers %d, sweep %d: want several times more by pairs",
			n, len(byMarkers.Failed), len(sw.Failed))
	}
	f := byPairs.Failed[0]
	if len(f.Path.Forks) == 0 || f.Path.Forks[len(f.Path.Forks)-1].Step == 0 {
		t.Errorf("the failure %v isn't a fork at a step", f.Path)
	}
	if r := dsttest.RunPath(f.Path, o, twoRareUnmarked, nil); r.Err == nil {
		t.Errorf("path %v passed on replay", f.Path)
	}
}

// twoRareHidden is twoRare visible only through an abstract state: no
// marker, and the goroutine waits at the same line either way, so neither
// markers nor interleaving pairs can tell a run that had A.
func twoRareHidden(h *dsttest.Harness) {
	a := false
	h.State(func() uint64 {
		if a {
			return 1
		}
		return 0
	})
	h.Go(func(w *dst.World) {
		for round := range 120 {
			switch round {
			case 10:
				a = w.Rand().IntN(100) == 0
			case 100:
				if a && w.Rand().IntN(10) == 0 {
					h.Fail(errors.New("A, then B"))
				}
			}
			w.Rand().Uint64()
			w.Sleep(time.Millisecond)
		}
	})
}

// Verifies: [states-explored]
// With the rare event visible only as an abstract state, exploring by
// states finds the bug after it several times more often than a sweep, and
// than exploring by markers and pairs (which see nothing): expected about
// 10 failing runs against 1. The sweep's report counts the two states.
// Fails if exploring by states doesn't win that clearly, if a failure
// isn't a fork at a step, if its path doesn't replay, or if the state
// count is off.
func TestExploreByStates(t *testing.T) {
	o := exploreOpts
	o.Seeds, o.Explore, o.KeepGoing = 1000, 1000, true
	sw := dsttest.Sweep(o, twoRareHidden)
	if sw.States != 2 {
		t.Errorf("the sweep reached %d distinct states, want 2 (A or not)", sw.States)
	}
	o.ExploreBy = "markers,order,pairs"
	blind := dsttest.Explore(o, twoRareHidden)
	o.ExploreBy = "states"
	byStates := dsttest.Explore(o, twoRareHidden)
	t.Logf("failing runs: by states %d, by markers/order/pairs %d, sweep %d", len(byStates.Failed), len(blind.Failed), len(sw.Failed))
	if n := len(byStates.Failed); n < 3 || n < 3*len(sw.Failed) || n < 3*len(blind.Failed) {
		t.Fatalf("by states %d failing runs, by the other signals %d, sweep %d: want several times more by states",
			n, len(blind.Failed), len(sw.Failed))
	}
	f := byStates.Failed[0]
	if len(f.Path.Forks) == 0 || f.Path.Forks[len(f.Path.Forks)-1].Step == 0 {
		t.Errorf("the failure %v isn't a fork at a step", f.Path)
	}
	if r := dsttest.RunPath(f.Path, o, twoRareHidden, nil); r.Err == nil {
		t.Errorf("path %v passed on replay", f.Path)
	}
}

// decoys: a twentieth of root seeds are dead ends rich in rare states (30
// states of their own, which no other root reaches, then nothing), and a hundredth reach a rare
// state A after which a 1-in-10 event fails. Forks of a dead end find
// nothing new. Which roots are dead ends is recorded in deadEnds.
func decoys(deadEnds *sync.Map) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		dead := h.World().Rand().Stream("kind").IntN(20) == 0
		deadEnds.Store(h.Seed(), dead)
		state, a := uint64(0), false
		h.State(func() uint64 { return state })
		h.Go(func(w *dst.World) {
			for round := range 120 {
				switch {
				case dead && round >= 10 && round < 40:
					state = h.Seed()<<8 | uint64(round) // states of this root's own
				case round == 10 && w.Rand().IntN(100) == 0:
					a, state = true, 1
				case round == 100 && a && w.Rand().IntN(10) == 0:
					h.Fail(errors.New("A, then B"))
				}
				w.Rand().Uint64()
				w.Sleep(time.Millisecond)
			}
		})
	}
}

// Verifies: [explore-spreads]
// Exploration doesn't dig into one root seed for long: forks of a root
// that find nothing new are capped (ExploreRootBudget), so dead-end roots
// rich in rare states don't soak up the forks, and the bug behind the
// rare state A is found several times more often than without the cap
// (expected about 10 failing runs against 2). Fails if a dead-end root is
// forked more than the budget, if the cap doesn't win clearly, or if a
// failing path doesn't replay.
func TestExploreSpreadsOverRoots(t *testing.T) {
	o := exploreOpts
	o.Explore, o.KeepGoing, o.ExploreBy = 1000, true, "states"
	explore := func(rootBudget int) (dsttest.Report, int) {
		var dead sync.Map
		o.ExploreRootBudget = rootBudget
		rep := dsttest.Explore(o, decoys(&dead))
		perRoot := map[uint64]int{}
		for _, r := range rep.Results {
			if v, _ := dead.Load(r.Path.Seed); v == true && len(r.Path.Forks) > 0 {
				perRoot[r.Path.Seed]++
			}
		}
		most := 0
		for _, n := range perRoot {
			most = max(most, n)
		}
		return rep, most
	}
	capped, most := explore(0) // the default, 8
	uncapped, mostUncapped := explore(1 << 30)
	t.Logf("failing runs: capped %d (a dead-end root forked at most %d times), uncapped %d (%d)",
		len(capped.Failed), most, len(uncapped.Failed), mostUncapped)
	if most > 8 {
		t.Errorf("a dead-end root was forked %d times, want at most 8 (forks finding nothing new)", most)
	}
	if n := len(capped.Failed); n < 3 || n < 3*len(uncapped.Failed) {
		t.Fatalf("capped %d failing runs, uncapped %d: want several times more with root budgets", n, len(uncapped.Failed))
	}
	f := capped.Failed[0]
	var dead sync.Map
	if r := dsttest.RunPath(f.Path, o, decoys(&dead), nil); r.Err == nil {
		t.Errorf("path %v passed on replay", f.Path)
	}
}

// An exploration is the same whatever the parallelism: the same paths, in
// the same order, by markers and by every signal. By markers, each of the
// two markers is forked from its budget's worth of times. Fails if they
// differ, or the fork count is off.
func TestExploreDeterministic(t *testing.T) {
	all := func(par int) ([]string, int) {
		o := exploreOpts
		o.Explore, o.Parallel, o.KeepGoing = 200, par, true
		var out []string
		stepForks := 0
		for _, r := range dsttest.Explore(o, twoRareUnmarked).Results {
			out = append(out, r.Path.String())
			for _, f := range r.Path.Forks {
				if f.Marker == "" && f.Step > 0 {
					stepForks++
				}
			}
		}
		return out, stepForks
	}
	a1, stepForks := all(1)
	if a8, _ := all(8); !slices.Equal(a1, a8) {
		t.Fatalf("explorations by every signal differ with 1 and 8 at once")
	}
	// Not vacuous: with no markers, it forked at steps (pairs).
	if stepForks < 50 {
		t.Errorf("exploring by every signal forked at a step %d times in 200 runs, want most of the non-fresh ones", stepForks)
	}
	run := func(par int) []string {
		o := exploreOpts
		o.Explore, o.Parallel, o.KeepGoing, o.ExploreBy = 200, par, true, "markers"
		o.ExploreRootBudget = 1 << 30 // feature budgets alone (TestExploreSpreadsOverRoots has root budgets)
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
