package dsttest

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/prince776/go-dst-kit/dstbugs"
)

// A Plant is a bug planted behind dstbugs.Bug, and the checks that must
// catch it.
type Plant struct {
	// Bug is what $DSTBUG is set to for the plant's sweep: a switch name,
	// or several, comma-separated, for a bug that takes more than one.
	Bug string
	// Checks name the Always and Final checks meant to catch it: the
	// sweep must fail, and every failing seed must fail one of them.
	Checks []string
	// Seeds, if not 0, replaces the sweep's Options.Seeds.
	Seeds int
	// Why is what the bug does, for the report.
	Why string
	// Options, if set, adjusts the sweep's options for this plant (its
	// faults, say); Setup, if set, replaces the setup.
	Options func(*Options)
	Setup   func(h *Harness)
}

// Plants shows each plant caught by its own checks: a subtest sweeps
// with no plant and must pass, then one subtest per plant sweeps with
// $DSTBUG set to it and must fail, every failing seed failing one of the
// plant's Checks (an Always invariant or a Final check of that name: the
// names the failures already carry). A failure for any other reason
// (another check, a deadlock, a panic) is reported, as is a plant whose
// switch the sweep never reached, and a plant naming a check the setup
// doesn't register.
//
// It needs a build with the dstbugs tag and skips without one. Set
// $DSTFIX (t.Setenv) first if candidate fixes should be on. A sweep stops
// at its first failure unless opts.KeepGoing: with it, every failing
// seed is checked.
func Plants(t *testing.T, opts Options, setup func(h *Harness), plants []Plant) {
	t.Helper()
	if !dstbugs.Tagged {
		t.Skip("planted bugs need a build with the dstbugs tag: go test -tags dstbugs")
	}
	t.Run("none", func(t *testing.T) {
		t.Setenv("DSTBUG", "")
		rep := Sweep(opts, setup)
		if !rep.OK() {
			for _, f := range rep.Failed {
				t.Errorf("seed %d failed with no plant: %s", f.Seed, firstLine(f.Err))
			}
			for _, n := range rep.Nondeterministic {
				t.Errorf("seed %d is nondeterministic", n.Seed)
			}
			for _, n := range rep.NeverHappened {
				t.Errorf("never happened: %s", n)
			}
		}
	})
	for _, p := range plants {
		t.Run(p.Bug, func(t *testing.T) {
			t.Setenv("DSTBUG", p.Bug)
			o, su := opts, setup
			if p.Seeds != 0 {
				o.Seeds = p.Seeds
			}
			if p.Options != nil {
				p.Options(&o)
			}
			if p.Setup != nil {
				su = p.Setup
			}
			var mu sync.Mutex
			registered := map[string]bool{}
			names := strings.Split(p.Bug, ",")
			before := hitsOf(names)
			rep := Sweep(o, func(h *Harness) {
				su(h)
				mu.Lock()
				defer mu.Unlock()
				for _, a := range h.always {
					registered[a.name] = true
				}
				for _, f := range h.final {
					registered[f.name] = true
				}
			})
			after := hitsOf(names)
			for _, problem := range plantProblems(p, rep, registered, before, after) {
				t.Error(problem)
			}
			if len(rep.Failed) > 0 {
				t.Logf("%s: %d of %d seeds failed; first: seed %d: %s", p.Bug, len(rep.Failed), rep.Runs, rep.Failed[0].Seed, firstLine(rep.Failed[0].Err))
			}
		})
	}
}

// hitsOf reads each switch's reach count.
func hitsOf(names []string) []int64 {
	hits := make([]int64, len(names))
	for i, n := range names {
		hits[i] = dstbugs.Hits(strings.TrimSpace(n))
	}
	return hits
}

// plantProblems says what is wrong with plant p's sweep rep: registered
// are the check names its setup registers, before and after the reach
// counts of its switches around the sweep.
func plantProblems(p Plant, rep Report, registered map[string]bool, before, after []int64) []string {
	var problems []string
	for _, c := range p.Checks {
		if !registered[c] {
			problems = append(problems, fmt.Sprintf("%s names check %q, which the setup doesn't register (Always or Final): registered %q", p.Bug, c, slices.Sorted(maps.Keys(registered))))
		}
	}
	for i, n := range strings.Split(p.Bug, ",") {
		if after[i] == before[i] {
			problems = append(problems, fmt.Sprintf("%s: the sweep never reached the switch %q (dstbugs.Bug): the workload can't trigger it", p.Bug, strings.TrimSpace(n)))
		}
	}
	if len(rep.Failed) == 0 {
		why := ""
		if p.Why != "" {
			why = " (" + p.Why + ")"
		}
		problems = append(problems, fmt.Sprintf("%s%s: no seed of %d failed", p.Bug, why, rep.Runs))
	}
	for _, f := range rep.Failed {
		if name, ok := checkName(f.Err); !ok || !slices.Contains(p.Checks, name) {
			problems = append(problems, fmt.Sprintf("%s: seed %d failed, but not by %q: %s", p.Bug, f.Seed, p.Checks, firstLine(f.Err)))
		}
	}
	for _, n := range rep.Nondeterministic {
		problems = append(problems, fmt.Sprintf("%s: seed %d is nondeterministic", p.Bug, n.Seed))
	}
	return problems
}

// checkName is the name of the Always or Final check err is a failure
// of.
func checkName(err error) (string, bool) {
	var inv *InvariantError
	if errors.As(err, &inv) {
		return inv.Name, true
	}
	var fin *FinalError
	if errors.As(err, &fin) {
		return fin.Name, true
	}
	return "", false
}

func firstLine(err error) string {
	s, _, _ := strings.Cut(err.Error(), "\n")
	if len(s) > 300 {
		s = s[:300] + "…"
	}
	return s
}
