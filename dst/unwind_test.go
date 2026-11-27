package dst_test

import (
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// Verifies: [kill-unwind-is-silent]
// Fails if: a killed goroutine's deferred Unlock of a mutex it had
// released (Go's "defer Unlock, Unlock, wait, Lock" pattern, killed in
// the wait) crashes the process; a deferred RWMutex Unlock or RUnlock
// the same way does; unwinding a killed machine fails the run (a held
// mutex's deferred Unlock, a WaitGroup.Done the kill drives negative);
// or, the negative case, a live goroutine's panicking defer no longer
// fails the run.
func TestKillUnwindIsSilent(t *testing.T) {
	for seed := uint64(1); seed <= 20; seed++ {
		s := simworld.New(simworld.Options{Seed: seed})
		a := s.Machine("a")
		res := s.Run(func(w *dst.World) {
			aw := a.World
			wake := dst.MakeChan[struct{}](aw, 0)
			var mu dst.Mutex
			var rw dst.RWMutex
			var held dst.Mutex
			var wg dst.WaitGroup
			aw.GoNamed("unlocked", func() {
				mu.Lock()
				defer mu.Unlock()
				mu.Unlock()
				wake.Recv()
				mu.Lock()
			})
			aw.GoNamed("runlocked", func() {
				rw.RLock()
				defer rw.RUnlock()
				rw.RUnlock()
				wake.Recv()
				rw.RLock()
			})
			aw.GoNamed("wunlocked", func() {
				rw.Lock()
				defer rw.Unlock()
				rw.Unlock()
				wake.Recv()
				rw.Lock()
			})
			aw.GoNamed("held", func() {
				held.Lock()
				defer held.Unlock()
				wake.Recv()
			})
			aw.GoNamed("done", func() {
				defer wg.Done() // never Added: negative once unwound
				wake.Recv()
			})
			w.Sleep(time.Millisecond)
			s.S.Crash(a.M)
		})
		if res.Err != nil {
			t.Fatalf("seed %d: a killed machine's unwinding failed the run: %v", seed, res.Err)
		}
	}
	// Negative case: a live goroutine's panicking defer still fails.
	res := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		var wg dst.WaitGroup
		func() {
			defer wg.Done()
		}()
	})
	if res.Err == nil || !strings.Contains(res.Err.Error(), "negative WaitGroup counter") {
		t.Fatalf("a live goroutine's panicking defer: got %v, want the run to fail", res.Err)
	}
}
