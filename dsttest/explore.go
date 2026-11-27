package dsttest

import (
	"flag"
	"math/rand/v2"
	"slices"
	"time"
)

var flagExplore = flag.Int("dst.explore", 0, "explore with this many guided runs instead of sweeping seeds (see Explore)")

// Explore is guided exploration: instead of only fresh seeds, a share of
// runs continue from where some run first reached a rare Sometimes marker,
// with fresh randomness from there (a fork). A marker reached by few runs
// is a state the sweep rarely gets to; going on from it many times tries
// what can happen next, which a fresh seed would have to reach the state
// first to try (Antithesis treats sometimes-assertions as checkpoints the
// same way).
//
// Runs go in generations of ExploreBatch: each generation's paths are
// chosen from the results of the earlier ones only, so an exploration
// replays from its options alone, whatever the parallelism, and every run
// in it is a path that replays on its own. A run is a fresh seed with
// probability ExploreFresh; otherwise a marker is drawn with weight
// 1/(runs that reached it), AFLFast's power schedule, among markers forked
// from fewer than ExploreBudget times, and the run forks the first run
// that reached it (one of the first few), just after it did. Markers are
// the test's own Sometimes, placed where something interesting happened;
// fault-point firings are coverage only. Forks keep the root seed's fault mix
// and features.
//
// It runs opts.Explore runs (default opts.Seeds) and reports as Sweep
// does, Results in run order.
func Explore(opts Options, setup func(h *Harness)) Report {
	opts.defaults()
	start := time.Now()
	n := opts.Explore
	if n <= 0 {
		n = opts.Seeds
	}
	rng := rand.New(rand.NewPCG(opts.FirstSeed, 0xe9a10e))
	type marker struct {
		reached int    // runs that reached it
		uses    int    // runs forked from it
		from    []Path // runs that reached it after their last fork (the first few)
	}
	markers := map[string]*marker{}
	nextSeed := opts.FirstSeed
	fresh := func() Path { p := Path{Seed: nextSeed}; nextSeed++; return p }

	var results []Result
	var divs []*Divergence
	for gen := 0; len(results) < n; gen++ {
		batch := make([]Path, min(opts.ExploreBatch, n-len(results)))
		names := slices.Sorted(mapsKeys(markers))
		for i := range batch {
			if gen == 0 || rng.Float64() < opts.ExploreFresh {
				batch[i] = fresh()
				continue
			}
			// Draw a marker: weight 1/reached, under budget.
			total := 0.0
			for _, name := range names {
				if m := markers[name]; len(m.from) > 0 && m.uses < opts.ExploreBudget {
					total += 1 / float64(m.reached)
				}
			}
			if total == 0 {
				batch[i] = fresh()
				continue
			}
			x := rng.Float64() * total
			for _, name := range names {
				m := markers[name]
				if len(m.from) == 0 || m.uses >= opts.ExploreBudget {
					continue
				}
				if x -= 1 / float64(m.reached); x < 0 {
					m.uses++
					from := m.from[rng.IntN(len(m.from))]
					batch[i] = from.Then(Fork{Marker: name, N: 1, Seed: rng.Uint64()>>1 + 1})
					break
				}
			}
			if batch[i].Seed == 0 { // rounding at the end of the walk
				batch[i] = fresh()
			}
		}
		checks := 0
		if gen == 0 {
			checks = opts.DeterminismChecks
		}
		rs, ds := runAll(batch, opts, setup, checks)
		stop := false
		for i, r := range rs {
			if r.Seed == 0 {
				continue
			}
			results, divs = append(results, r), append(divs, ds[i])
			if r.Err != nil {
				stop = !opts.KeepGoing
			}
			last := uint64(0)
			if k := len(r.ForkSteps); k > 0 {
				last = r.ForkSteps[k-1]
			}
			for _, name := range slices.Sorted(mapsKeys(r.FirstMarks)) {
				m := markers[name]
				if m == nil {
					m = &marker{}
					markers[name] = m
				}
				m.reached++
				// Keep the first few runs that reached it after their own
				// last fork (a fork applies only after the earlier ones)
				// with forks to spare: forks pick among them, so they don't
				// all descend from one root.
				if len(m.from) < carriers && r.FirstMarks[name] > last && len(r.Path.Forks) < maxForks {
					m.from = append(m.from, r.Path)
				}
			}
		}
		if stop {
			break
		}
	}
	rep := aggregate(opts, results, divs)
	rep.Wall = time.Since(start)
	return rep
}

const (
	// maxForks bounds a path's forks, so exploration doesn't only dig deeper.
	maxForks = 8
	// carriers is how many runs that reached a marker forks choose among.
	carriers = 8
)
