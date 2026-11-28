//go:build dstbugs

package dstbugs_test

import (
	"sync"
	"testing"

	"github.com/prince776/go-dst-kit/dstbugs"
)

// Verifies: [on-reads-env], [hits-count-reaches]
// Fails if: a listed name isn't on, an unlisted one (a prefix, the
// other variable's, the empty name) is, a change to the environment
// isn't seen at the next call, or Hits misses a reach (an off one, a
// Fix, one from another goroutine). Runs only with the tag:
// TestTaggedBuild runs it from the normal build.
func TestOnReadsEnv(t *testing.T) {
	if !dstbugs.Tagged {
		t.Fatal("Tagged is false in the tagged build")
	}
	t.Setenv("DSTBUG", "lost-ack, torn-tail")
	t.Setenv("DSTFIX", "deliver-unlocked")
	for _, c := range []struct {
		name     string
		bug, fix bool
	}{
		{"lost-ack", true, false},
		{"torn-tail", true, false},
		{"lost", false, false},
		{"deliver-unlocked", false, true},
		{"", false, false},
	} {
		if got := dstbugs.Bug(c.name); got != c.bug {
			t.Errorf("Bug(%q) = %v, want %v", c.name, got, c.bug)
		}
		if got := dstbugs.Fix(c.name); got != c.fix {
			t.Errorf("Fix(%q) = %v, want %v", c.name, got, c.fix)
		}
	}
	t.Setenv("DSTBUG", "")
	if dstbugs.Bug("lost-ack") {
		t.Error("Bug didn't follow $DSTBUG's change")
	}

	before := dstbugs.Hits("counted")
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 100 {
				dstbugs.Bug("counted")
				dstbugs.Fix("counted")
			}
		}()
	}
	wg.Wait()
	if got := dstbugs.Hits("counted") - before; got != 1600 {
		t.Errorf("Hits counted %d reaches, want 1600", got)
	}
	if got := dstbugs.Hits("never-reached"); got != 0 {
		t.Errorf("Hits of a switch never reached = %d", got)
	}
}
