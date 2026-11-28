package dst

import (
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Verifies: [wait-follows-done]
// Outside a simulation Wait is the sync.WaitGroup's alone: a simulation's
// count reaching zero doesn't let it return.
// Fails if: Wait returns while the sync.WaitGroup still counts (it read
// the simulation's count), or, the negative case, doesn't return once
// the last Done finishes.
func TestWaitFollowsDone(t *testing.T) {
	var g WaitGroup
	g.Add(1)
	g.n.Add(-1) // the simulation's count, not the real World's
	returned := make(chan struct{})
	go func() {
		g.Wait()
		close(returned)
	}()
	select {
	case <-returned:
		t.Fatal("Wait returned while a Done was still partway")
	case <-time.After(50 * time.Millisecond):
	}
	g.wg.Add(-1) // the real Done
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait didn't return after the last Done")
	}
}

// Verifies: [real-is-std]
// A primitive only real goroutines use never records a scheduler, even
// while a simulation runs in the process (sched.Active is true): so its
// calls never reach the cached-scheduler check (HeldByCaller) or the
// sharing check, only the registry lookup that says "not simulated".
// Fails if a real goroutine's Lock, TryLock, RLock, Unlock or Wait
// leaves a scheduler in the primitive.
func TestRealUseNeverCaches(t *testing.T) {
	s := sched.New(sched.Options{Seed: 1, Watchdog: time.Hour})
	m := s.NewMachine("m")
	block := make(chan struct{})
	s.SpawnRoot(m, "blocked", func() { <-block }) // a raw wait: keeps the run alive
	ran := make(chan struct{})
	go func() { s.Run(func() bool { return false }); close(ran) }()
	defer func() { close(block); <-ran }()
	for !sched.Active() {
		time.Sleep(time.Millisecond)
	}
	var (
		mu Mutex
		rw RWMutex
		wg WaitGroup
		c  = NewCond(&mu)
	)
	for range 100 {
		mu.Lock()
		mu.Unlock()
		if mu.TryLock() {
			mu.Unlock()
		}
		rw.RLock()
		rw.RUnlock()
		rw.Lock()
		rw.Unlock()
		if rw.TryRLock() {
			rw.RUnlock()
		}
		wg.Add(1)
		wg.Done()
		wg.Wait()
		c.Signal()
	}
	if mu.s.Load() != nil || rw.s.Load() != nil || wg.s.Load() != nil || c.s.Load() != nil {
		t.Fatal("a real goroutine's use recorded a scheduler")
	}
}
