package sched

import (
	"testing"
	"time"
)

// A task forgets its goroutine's g as the goroutine exits, before the
// scheduler takes the baton back and clears running: the runtime then
// reuses that g for a new goroutine, which must not match the task in
// HeldByCaller, however briefly.
// Fails if a finished task still records a g.
func TestFinishedTaskForgetsItsG(t *testing.T) {
	s := New(Options{Seed: 1, Watchdog: time.Minute})
	m := s.NewMachine("m")
	for range 50 {
		s.SpawnRoot(m, "short", func() {})
	}
	s.Run(func() bool {
		for _, t := range s.tasks {
			if t.state != finished {
				return false
			}
		}
		return true
	})
	for _, task := range s.tasks {
		if g := task.goidShared.Load(); g != 0 {
			t.Fatalf("finished task %s still records g %#x", task, g)
		}
	}
}
