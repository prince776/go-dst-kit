package pool

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"spike/dst"
)

var cfg = Config{Workers: 3, Jobs: 20, QueueCap: 2, MaxAttempts: 3, FailProb: 0.3}

// checkResults is the oracle: every job exactly once, attempts in range.
func checkResults(out []Result, cfg Config) error {
	seen := map[int]bool{}
	for _, r := range out {
		if seen[r.ID] || r.ID < 0 || r.ID >= cfg.Jobs || r.Attempts < 1 || r.Attempts > cfg.MaxAttempts {
			return fmt.Errorf("bad result %+v (seen before: %v)", r, seen[r.ID])
		}
		seen[r.ID] = true
	}
	if len(seen) != cfg.Jobs {
		return fmt.Errorf("%d of %d jobs finished", len(seen), cfg.Jobs)
	}
	return nil
}

func runBuggy(seed uint64) dst.Result {
	var oracle error
	res := dst.Run(seed, func(w dst.World) { oracle = checkResults(Buggy(w, cfg), cfg) })
	if res.Err == nil {
		res.Err = oracle
	}
	return res
}

func runFixed(seed uint64) dst.Result {
	var oracle error
	res := dst.Run(seed, func(w dst.World) {
		out, err := Fixed(w, cfg)
		if err == nil {
			err = checkResults(out, cfg)
		}
		oracle = err
	})
	if res.Err == nil {
		res.Err = oracle
	}
	return res
}

func kind(err error) string {
	switch s := err.Error(); {
	case strings.HasPrefix(s, "deadlock"):
		return "deadlock"
	case strings.Contains(s, "send on closed channel"):
		return "panic: send on closed channel"
	default:
		return s
	}
}

func TestSimFindsBugsInBuggy(t *testing.T) {
	first := map[string]uint64{}
	count := map[string]int{}
	var steps uint64
	var simTime time.Duration
	start := time.Now()
	const seeds = 2000
	for seed := uint64(1); seed <= seeds; seed++ {
		r := runBuggy(seed)
		steps += r.Steps
		simTime += r.SimTime
		if r.Err != nil {
			k := kind(r.Err)
			count[k]++
			if _, ok := first[k]; !ok {
				first[k] = seed
			}
		}
	}
	wall := time.Since(start)
	t.Logf("%d seeds in %v: %d baton handoffs (%.0f/s), %v simulated (%.0fx)", seeds, wall.Round(time.Millisecond), steps, float64(steps)/wall.Seconds(), simTime.Round(time.Second), float64(simTime)/float64(wall))
	for k, n := range count {
		t.Logf("%-32s %4d seeds, first: seed %d", k, n, first[k])
	}
	for _, want := range []string{"deadlock", "panic: send on closed channel"} {
		seed, ok := first[want]
		if !ok {
			t.Errorf("no seed found %q", want)
			continue
		}
		// Replay: the same seed gives the same failure and the same run.
		a, b := runBuggy(seed), runBuggy(seed)
		// (The error's first line; a panic's stack trace below it has goroutine
		// numbers and addresses that differ between runs.)
		line := func(err error) string { return strings.SplitN(err.Error(), "\n", 2)[0] }
		if a.Fingerprint != b.Fingerprint || kind(a.Err) != want || line(a.Err) != line(b.Err) {
			t.Errorf("seed %d didn't replay identically: %x %x\n%v\n%v", seed, a.Fingerprint, b.Fingerprint, line(a.Err), line(b.Err))
		}
	}
	if seed, ok := first["deadlock"]; ok {
		t.Logf("replay of seed %d:\n%v", seed, runBuggy(seed).Err)
	}
	if seed, ok := first["panic: send on closed channel"]; ok {
		t.Logf("replay of seed %d:\n%s", seed, strings.Join(strings.Split(runBuggy(seed).Err.Error(), "\n")[:12], "\n"))
	}
}

func TestSimFixedIsClean(t *testing.T) {
	for seed := uint64(1); seed <= 2000; seed++ {
		if r := runFixed(seed); r.Err != nil {
			t.Fatalf("seed %d: %v", seed, r.Err)
		}
	}
}

// The same code runs for real: real goroutines, real sleeps.
func TestRealWorldFixed(t *testing.T) {
	done := make(chan error, 1)
	start := time.Now()
	go func() {
		out, err := Fixed(dst.Real{}, cfg)
		if err == nil {
			err = checkResults(out, cfg)
		}
		done <- err
	}()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	t.Logf("real run took %v", time.Since(start).Round(time.Millisecond))
}

// Fingerprints don't depend on how many OS threads Go uses.
func TestDeterministicAcrossGOMAXPROCS(t *testing.T) {
	sum := func() uint64 {
		var h uint64
		for seed := uint64(1); seed <= 300; seed++ {
			h = h*31 + runBuggy(seed).Fingerprint
			h = h*31 + runFixed(seed).Fingerprint
		}
		return h
	}
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(0))
	runtime.GOMAXPROCS(1)
	one := sum()
	runtime.GOMAXPROCS(8)
	eight := sum()
	if one != eight {
		t.Fatalf("GOMAXPROCS=1 gives %x, GOMAXPROCS=8 gives %x", one, eight)
	}
	t.Logf("600 runs identical under GOMAXPROCS=1 and 8 (%x)", one)
}
