package dsttest_test

import (
	"slices"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// forkWorkload draws from every kind of stream (machine Rand, a named
// Stream, a fault point's calls) and records configuration (a feature,
// the clock), with a Sometimes marker at its 50th round.
type forkObs struct {
	before, after []uint64 // every draw, before/after round 50
	// after round 50, each stream on its own: machine Rand, a Stream, a
	// fault point's outcomes
	rnd, str, pt []uint64
	feature      bool
	clockSkew    time.Duration
}

func forkWorkload(obs *forkObs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		obs.feature = h.Feature("f")
		h.Machine("m", func(w *dst.World) {
			obs.clockSkew = w.Now().Sub(time.Time{}) % time.Hour
			for i := 0; h.Phase() == dsttest.Faulty; i++ {
				a, b, c := w.Rand().Uint64(), w.Rand().Stream("s").Uint64(), uint64(0)
				if dst.Inject(w, "svc.op") == dst.Fail {
					c = 1
				}
				if i < 50 {
					obs.before = append(obs.before, a, b, c)
				} else {
					obs.after = append(obs.after, a, b, c)
					obs.rnd, obs.str, obs.pt = append(obs.rnd, a), append(obs.str, b), append(obs.pt, c)
				}
				if i == 49 {
					h.Sometimes("round 50")
				}
				w.Sleep(time.Millisecond)
			}
		})
	}
}

var forkOpts = dsttest.Options{Faults: dsttest.Faults{Points: true}, FaultPhase: 2 * time.Second, RecoveryPhase: 10 * time.Millisecond,
	Points: map[string]dsttest.Point{"svc.": {Fail: 0.5}}}

// Verifies: a fork keeps the run up to its anchor and the configuration,
// and reseeds every stream of choices after it.
// Fails if: draws before the anchor differ, any stream's draws after it
// are unchanged, the configuration (feature, clock) changes, a path
// doesn't replay exactly, or an unreached anchor isn't reported.
func TestFork(t *testing.T) {
	var base, forked, again forkObs
	rb := dsttest.RunPath(dsttest.Path{Seed: 3}, forkOpts, forkWorkload(&base), nil)
	p := dsttest.Path{Seed: 3}.Then(dsttest.Fork{Marker: "round 50", N: 1, Seed: 77})
	rf := dsttest.RunPath(p, forkOpts, forkWorkload(&forked), nil)
	dsttest.RunPath(p, forkOpts, forkWorkload(&again), nil)
	if rb.Err != nil || rf.Err != nil {
		t.Fatal(rb.Err, rf.Err)
	}
	if rf.Forked != 1 || rb.Forked != 0 {
		t.Fatalf("forks applied: forked %d, base %d", rf.Forked, rb.Forked)
	}
	if !slices.Equal(base.before, forked.before) {
		t.Errorf("the run before the anchor changed")
	}
	for _, st := range []struct {
		name      string
		base, got []uint64
	}{{"machine Rand", base.rnd, forked.rnd}, {"Rand().Stream", base.str, forked.str}, {"fault point", base.pt, forked.pt}} {
		if len(st.got) < 100 || slices.Equal(st.base, st.got) {
			t.Errorf("after the fork the %s stream is unchanged (%d draws): it wasn't reseeded", st.name, len(st.got))
		}
	}
	if base.feature != forked.feature || base.clockSkew != forked.clockSkew {
		t.Errorf("the fork changed the configuration: feature %v/%v, clock %v/%v", base.feature, forked.feature, base.clockSkew, forked.clockSkew)
	}
	if !slices.Equal(forked.after, again.after) {
		t.Errorf("a path didn't replay exactly")
	}

	// A fork whose anchor never comes changes nothing, and is reported.
	var never forkObs
	rn := dsttest.RunPath(dsttest.Path{Seed: 3}.Then(dsttest.Fork{Marker: "no such marker", N: 1, Seed: 77}), forkOpts, forkWorkload(&never), nil)
	if rn.Forked != 0 || !slices.Equal(never.after, base.after) {
		t.Errorf("an unreached fork: applied %d, run unchanged %v", rn.Forked, slices.Equal(never.after, base.after))
	}

	// A step anchor: the same before the step, different after.
	var stepped forkObs
	rs := dsttest.RunPath(dsttest.Path{Seed: 3}.Then(dsttest.Fork{Step: rb.Steps / 2, Seed: 5}), forkOpts, forkWorkload(&stepped), nil)
	if len(rb.Checkpoints) < 2 {
		t.Fatalf("%d steps, %d checkpoints: too short a run", rb.Steps, len(rb.Checkpoints))
	}
	// (The trace hashes scheduling, not values: one goroutine's schedule
	// is forced, so compare what was drawn.)
	n := len(stepped.after)
	if rs.Forked != 1 || !slices.Equal(stepped.before, base.before) || slices.Equal(stepped.after[n/2:], base.after[n/2:]) {
		t.Errorf("a step fork: applied %d, before kept %v, end changed %v", rs.Forked,
			slices.Equal(stepped.before, base.before), !slices.Equal(stepped.after[n/2:], base.after[n/2:]))
	}
	if m, ok := rb.FirstMarks["round 50"]; !ok || m == 0 {
		t.Errorf("first marks: %v", rb.FirstMarks)
	}
}

// Path's text form round-trips, markers quoted.
func TestParsePath(t *testing.T) {
	for _, s := range []string{"17", `17/"leader elected"#2:99`, "17/@4821:99", `5/"a \"quoted\" / one"#1:2/@10:3`} {
		p, err := dsttest.ParsePath(s)
		if err != nil {
			t.Fatalf("%s: %v", s, err)
		}
		if p.String() != s {
			t.Errorf("%s round-trips to %s", s, p.String())
		}
	}
	for _, s := range []string{"", "0", "x", "17/", "17/@:3", `17/"m"#0:3`, `17/"m:3`, "17/@5"} {
		if _, err := dsttest.ParsePath(s); err == nil {
			t.Errorf("%q parsed", s)
		}
	}
}
