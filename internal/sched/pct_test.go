package sched_test

import (
	"math/rand/v2"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// Verifies: [pct-is-legal]
// Fails if: PCT scheduling produces an outcome Go's semantics don't allow,
// for any of the generated channel programs.
func TestPCTIsLegal(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	checked := 0
	for range 150 {
		p := genProgram(r)
		want := explore(p)
		if len(want) > 40 {
			continue
		}
		checked++
		for seed := uint64(1); seed <= 150; seed++ {
			o := runSimWith(p, simworld.Options{Seed: seed, PCTDepth: 1 + int(seed%3), PCTHorizon: 50})
			if !want[o] {
				t.Fatalf("program %v\nPCT (seed %d) produced %q, which Go can't; Go allows:\n  %s", p, seed, o, strings.Join(sortedKeys(want), "\n  "))
			}
		}
	}
	if checked < 100 {
		t.Fatalf("only %d programs checked", checked)
	}
}

// starved reports whether, in one run, goroutine B's first operation came
// only after A finished all 50 of its sends: a "bug" that needs B starved
// for 50 steps in a row.
func starved(opts simworld.Options) bool {
	bug := false
	r := simworld.Run(opts, func(w *dst.World) {
		ch := dst.MakeChan[int](w, 100)
		other := dst.MakeChan[int](w, 1)
		var wg dst.WaitGroup
		wg.Go(w, func() { // B
			other.Send(1)
			bug = ch.Len() == 50
		})
		wg.Go(w, func() { // A
			for i := range 50 {
				ch.Send(i)
			}
		})
		wg.Wait()
	})
	if r.Err != nil {
		panic(r.Err)
	}
	return bug
}

// Verifies: [pct-finds-starvation]
// Fails if: uniform scheduling finds the starvation bug (the test would be
// meaningless), or PCT doesn't find it within 50 seeds.
func TestPCTFindsStarvation(t *testing.T) {
	for seed := uint64(1); seed <= 1000; seed++ {
		if starved(simworld.Options{Seed: seed}) {
			t.Fatalf("uniform scheduling found it at seed %d: not a hard enough bug", seed)
		}
	}
	found := 0
	for seed := uint64(1); seed <= 50; seed++ {
		if starved(simworld.Options{Seed: seed, PCTDepth: 1 + int(seed%3)}) {
			found++
		}
	}
	if found == 0 {
		t.Fatal("PCT never starved B in 50 seeds")
	}
	t.Logf("uniform: 0 of 1000 seeds; PCT: %d of 50", found)
}

// Verifies: [pct-no-livelock]
// Fails if: a highest-priority goroutine polling for a lower one's flag
// spins forever instead of being demoted.
func TestPCTNoLivelock(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		r := simworld.Run(simworld.Options{Seed: seed, PCTDepth: 1, MaxSteps: 100000}, func(w *dst.World) {
			flag := false
			var wg dst.WaitGroup
			wg.Go(w, func() {
				for !flag {
					dst.Select(w, dst.Default(nil)) // poll: a scheduling point, never blocks
				}
			})
			wg.Go(w, func() {
				w.Sleep(0)
				flag = true
			})
			wg.Wait()
		})
		if r.Err != nil {
			t.Fatalf("seed %d: %v", seed, r.Err)
		}
	}
}

// Verifies: [seed-replays] under PCT
// Fails if: a PCT seed's two runs differ, or different seeds all agree.
func TestPCTReplays(t *testing.T) {
	run := func(seed uint64) uint64 {
		return simworld.Run(simworld.Options{Seed: seed, PCTDepth: 3, PCTHorizon: 100}, func(w *dst.World) {
			ch := dst.MakeChan[int](w, 0)
			for i := range 4 {
				w.Go(func() {
					w.Sleep(time.Duration(w.Rand().IntN(3)) * time.Millisecond)
					ch.Send(i)
				})
			}
			for range 4 {
				ch.Recv()
			}
		}).Fingerprint
	}
	seen := map[uint64]bool{}
	for seed := uint64(1); seed <= 20; seed++ {
		a, b := run(seed), run(seed)
		if a != b {
			t.Fatalf("seed %d: fingerprints %x and %x", seed, a, b)
		}
		seen[a] = true
	}
	if len(seen) < 2 {
		t.Fatal("every seed scheduled identically")
	}
}
