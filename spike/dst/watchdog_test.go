package dst

import (
	"strings"
	"testing"
)

// Code that blocks on a raw channel (outside the World) is reported, not hung.
func TestWatchdogCatchesRawBlocking(t *testing.T) {
	raw := make(chan int)
	r := Run(1, func(w World) {
		w.Go("leaky", func() { <-raw }) // forgot to migrate this channel
		w.Sleep(1)
	})
	if r.Err == nil || !strings.Contains(r.Err.Error(), `"leaky"`) {
		t.Fatalf("got %v", r.Err)
	}
	t.Log(r.Err)
}
