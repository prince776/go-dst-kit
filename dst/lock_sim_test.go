package dst_test

import (
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// lostUpdate runs two goroutines that each read n in one critical section
// and write n+1 in another, and returns n (2, or 1 if an update was lost).
func lostUpdate(seed uint64, rw bool) int {
	n := 0
	simworld.Run(simworld.Options{Seed: seed}, func(w *dst.World) {
		var mu dst.Mutex
		var rmu dst.RWMutex
		var wg dst.WaitGroup
		for range 2 {
			wg.Go(w, func() {
				var v int
				if rw {
					rmu.RLock()
					v = n
					rmu.RUnlock()
					rmu.Lock()
					n = v + 1
					rmu.Unlock()
					return
				}
				mu.Lock()
				v = n
				mu.Unlock()
				mu.Lock()
				n = v + 1
				mu.Unlock()
			})
		}
		wg.Wait()
	})
	return n
}

// Verifies: [lock-is-a-scheduling-point]
// Fails if: no seed interleaves the two critical sections (the lost update
// is never found), or every seed does (the correct outcome never happens).
func TestLockIsASchedulingPoint(t *testing.T) {
	for _, rw := range []bool{false, true} {
		seen := map[int]int{}
		for seed := uint64(1); seed <= 200; seed++ {
			seen[lostUpdate(seed, rw)]++
		}
		if seen[1] == 0 || seen[2] == 0 {
			t.Fatalf("rw=%v: outcomes %v, want both 1 (lost update) and 2", rw, seen)
		}
	}
}
