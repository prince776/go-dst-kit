package sched

import (
	"sync"
	"testing"
)

// reuseAfter registers a task on a goroutine that then exits (deleting
// its entry first if forget), and starts goroutines until one runs on the
// same g. It returns what that goroutine's CurrentTask was, and the task.
func reuseAfter(t *testing.T, forget bool) (got, task *Task) {
	t.Helper()
	task = &Task{Name: "finished"}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		task.goid = goid()
		register(task)
		if forget {
			unregister(task)
		} else {
			active.Add(-1) // keep the entry, as a missing Delete would
		}
	}()
	wg.Wait()
	defer registry.Delete(task.goid)
	active.Add(1) // lookups happen while some simulation is active
	defer active.Add(-1)
	for range 100000 {
		var reused bool
		var cur *Task
		wg.Add(1)
		go func() {
			defer wg.Done()
			if goid() == task.goid {
				reused, cur = true, CurrentTask()
			}
		}()
		wg.Wait()
		if reused {
			return cur, task
		}
	}
	t.Fatal("no goroutine reused the finished goroutine's g in 100,000 tries: this test can't show anything")
	return nil, nil
}

// Verifies: [registry-forgets-finished]
// Fails if: a goroutine running on a finished task's g (the runtime
// reuses them) is identified as that task. The negative case: with the
// task's entry left in the registry, as a skipped Delete would, the same
// goroutine is identified as it, so the reuse this test relies on really
// happens.
func TestRegistryForgetsFinished(t *testing.T) {
	if got, task := reuseAfter(t, true); got == task {
		t.Errorf("a goroutine reusing a finished task's g was identified as that task")
	}
	if got, task := reuseAfter(t, false); got != task {
		t.Errorf("negative case: with the entry kept, the reusing goroutine wasn't identified as the old task (got %v): the test doesn't reach reuse", got)
	}
}
