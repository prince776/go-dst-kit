package dsttest

import (
	"cmp"
	"flag"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"time"
)

var (
	flagExplore   = flag.Int("dst.explore", 0, "explore with this many guided runs instead of sweeping seeds (see Explore)")
	flagExploreBy = flag.String("dst.explore.by", "", `what exploration steers by: a comma-separated set of "markers", "order" and "pairs" (default all)`)
)

// Explore is guided exploration: instead of only fresh seeds, a share of
// runs continue from where some run first did something rare, with fresh
// randomness from there (a fork). A state the sweep rarely gets to is one
// a fresh seed would have to reach again to try what can happen next;
// going on from it many times tries that directly (Antithesis treats
// sometimes-assertions as checkpoints the same way; AFL keeps inputs that
// reach new coverage).
//
// What counts as rare is a feature of a run, by ExploreBy:
//   - "markers": a Sometimes marker reached. Forks go just after its first
//     hit (a marker anchor: stable as code changes).
//   - "order": two markers first reached one after the other, a then b.
//     Forks go just after b's first hit.
//   - "pairs": an interleaving pair (Result.FirstPairs): one goroutine
//     handing off at one code site and the next resuming at another, an
//     ordering of the program's goroutines. Forks go just after the step
//     it first ran (a step anchor).
//
// Runs go in generations of ExploreBatch: each generation's paths are
// chosen from the results of the earlier ones only, so an exploration
// replays from its options alone, whatever the parallelism, and every run
// in it is a path that replays on its own. A run is a fresh seed with
// probability ExploreFresh; otherwise a feature is drawn with weight
// 1/(runs that had it), AFLFast's power schedule, among features forked
// from fewer than ExploreBudget times, and the run forks one of the first
// few runs that had it after their own last fork. Fault-point firings are
// coverage only. Forks keep the root seed's fault mix and features.
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
	by := map[string]bool{}
	for _, k := range strings.Split(opts.ExploreBy, ",") {
		by[strings.TrimSpace(k)] = true
	}
	opts.recordPairs = by["pairs"]
	rng := rand.New(rand.NewPCG(opts.FirstSeed, 0xe9a10e))
	type carrier struct {
		path Path
		fork Fork // where to fork it (Seed unset)
	}
	type feature struct {
		reached int       // runs that had it
		uses    int       // runs forked from it
		from    []carrier // the first few runs that had it after their last fork
	}
	feats := map[string]*feature{}
	nextSeed := opts.FirstSeed
	fresh := func() Path { p := Path{Seed: nextSeed}; nextSeed++; return p }
	open := func(f *feature) bool { return len(f.from) > 0 && f.uses < opts.ExploreBudget }

	var results []Result
	var divs []*Divergence
	for gen := 0; len(results) < n; gen++ {
		batch := make([]Path, min(opts.ExploreBatch, n-len(results)))
		names := slices.Sorted(mapsKeys(feats))
		for i := range batch {
			if gen == 0 || rng.Float64() < opts.ExploreFresh {
				batch[i] = fresh()
				continue
			}
			// Draw a feature: weight 1/reached, under budget.
			total := 0.0
			for _, name := range names {
				if f := feats[name]; open(f) {
					total += 1 / float64(f.reached)
				}
			}
			if total == 0 {
				batch[i] = fresh()
				continue
			}
			x := rng.Float64() * total
			for _, name := range names {
				f := feats[name]
				if !open(f) {
					continue
				}
				if x -= 1 / float64(f.reached); x < 0 {
					f.uses++
					c := f.from[rng.IntN(len(f.from))]
					fk := c.fork
					fk.Seed = rng.Uint64()>>1 + 1
					batch[i] = c.path.Then(fk)
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
			note := func(name string, at uint64, fk Fork) {
				f := feats[name]
				if f == nil {
					f = &feature{}
					feats[name] = f
				}
				f.reached++
				// Keep the first few runs that had it after their own last
				// fork (a fork applies only after the earlier ones) with
				// forks to spare: forks pick among them, so they don't all
				// descend from one root.
				if len(f.from) < carriers && at > last && len(r.Path.Forks) < maxForks {
					f.from = append(f.from, carrier{r.Path, fk})
				}
			}
			for _, ft := range runFeatures(r, by) {
				note(ft.name, ft.at, ft.fork)
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

type runFeature struct {
	name string
	at   uint64 // the step it first happened
	fork Fork   // where to fork to go on from it
}

// runFeatures is what a run did that exploration steers by, in a fixed
// order.
func runFeatures(r Result, by map[string]bool) []runFeature {
	var out []runFeature
	marks := slices.Sorted(mapsKeys(r.FirstMarks))
	if by["markers"] {
		for _, m := range marks {
			out = append(out, runFeature{"m:" + m, r.FirstMarks[m], Fork{Marker: m, N: 1}})
		}
	}
	if by["order"] && len(marks) > 1 {
		// Markers in the order first reached (ties by name).
		byStep := slices.Clone(marks)
		slices.SortStableFunc(byStep, func(a, b string) int { return cmp.Compare(r.FirstMarks[a], r.FirstMarks[b]) })
		for i := 1; i < len(byStep); i++ {
			a, b := byStep[i-1], byStep[i]
			out = append(out, runFeature{"o:" + a + "\x00" + b, r.FirstMarks[b], Fork{Marker: b, N: 1}})
		}
	}
	if by["pairs"] {
		for _, p := range slices.Sorted(maps.Keys(r.FirstPairs)) {
			at := r.FirstPairs[p]
			out = append(out, runFeature{"p:" + strconv.FormatUint(p, 16), at, Fork{Step: at}})
		}
	}
	return out
}

const (
	// maxForks bounds a path's forks, so exploration doesn't only dig deeper.
	maxForks = 8
	// carriers is how many runs that reached a marker forks choose among.
	carriers = 8
)
