package sched

import (
	"sync"
	"testing"
)

func BenchmarkGoid(b *testing.B) {
	for i := 0; i < b.N; i++ {
		goid()
	}
}

func BenchmarkGoidSlow(b *testing.B) {
	for i := 0; i < b.N; i++ {
		goidSlow()
	}
}

// Verifies: [goid-identifies]
// Fails if: goid changes between calls on one goroutine (also across a
// stack growth, which moves the stack but not the g), or two goroutines
// alive at the same time get the same value. The negative case: a
// function that returned a constant would pass the first half and fail
// the second; one that returned something per call (a counter) would fail
// the first.
func TestGoidIdentifies(t *testing.T) {
	const n = 64
	ids := make([]uint64, n)
	var started, release sync.WaitGroup
	started.Add(n)
	release.Add(1)
	var bad sync.Map
	for i := range n {
		go func() {
			first := goid()
			grow(200) // move this goroutine's stack
			if goid() != first {
				bad.Store(i, "changed after the stack grew")
			}
			ids[i] = first
			started.Done()
			release.Wait() // stay alive until every goroutine has its value
			if goid() != first {
				bad.Store(i, "changed while alive")
			}
		}()
	}
	started.Wait()
	seen := map[uint64]int{}
	for i, id := range ids {
		if j, dup := seen[id]; dup {
			t.Errorf("goroutines %d and %d, both alive, have the same goid %#x", j, i, id)
		}
		seen[id] = i
	}
	release.Done()
	bad.Range(func(k, v any) bool { t.Errorf("goroutine %v: goid %v", k, v); return true })
	if goid() != goid() {
		t.Error("goid changed between two calls on the test's goroutine")
	}
}

// grow uses enough stack to make the runtime copy it.
func grow(depth int) byte {
	var pad [512]byte
	pad[depth%512] = byte(depth)
	if depth == 0 {
		return pad[0]
	}
	return grow(depth-1) + pad[depth%512]
}
