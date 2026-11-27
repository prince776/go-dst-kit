package dst

import (
	"testing"
	"time"
)

// Verifies: [wait-follows-done]
// Fails if: Wait returns while a Done is partway (its count update made,
// the sync.WaitGroup's not yet), or, the negative case, doesn't return
// once that Done finishes.
func TestWaitFollowsDone(t *testing.T) {
	var g WaitGroup
	g.Add(1)
	g.n.Add(-1) // a Done preempted between its two updates
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
	g.wg.Add(-1) // the Done finishes
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatal("Wait didn't return after the last Done")
	}
}
