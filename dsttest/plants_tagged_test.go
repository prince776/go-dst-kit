//go:build dstbugs

package dsttest_test

import (
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dstbugs"
	"github.com/prince776/go-dst-kit/dsttest"
)

// counter is the system: workers add to a total under a lock, with two
// planted bugs. Each check is named, and catches its own bug only.
func counter(h *dsttest.Harness) {
	var mu dst.Mutex
	total, added := 0, 0
	h.Go(func(w *dst.World) {
		var wg dst.WaitGroup
		for range 4 {
			wg.Go(w, func() {
				for range 5 {
					mu.Lock()
					v := total
					if dstbugs.Bug("lost-update") {
						mu.Unlock()
						w.Yield()
						mu.Lock()
					}
					total = v + 1
					added++
					mu.Unlock()
					w.Sleep(time.Millisecond)
				}
			})
		}
		wg.Wait()
	})
	h.Always("never negative", func() error {
		if dstbugs.Bug("goes-negative") && added == 7 {
			total = -1
		}
		if total < 0 {
			return errors.New("total < 0")
		}
		return nil
	})
	h.Final("every add counted", func(w *dst.World) error {
		if total != added {
			return fmt.Errorf("total %d, added %d", total, added)
		}
		return nil
	})
}

// Runs from TestPlantsTaggedBuild. Fails if Plants doesn't pass a
// correct table: the baseline passing, each plant caught by its check.
func TestPlantsTagged(t *testing.T) {
	dsttest.Plants(t, dsttest.Options{Seeds: 20, KeepGoing: true}, counter, []dsttest.Plant{
		{Bug: "lost-update", Checks: []string{"every add counted"}, Why: "a read-modify-write across an unlock"},
		{Bug: "goes-negative", Checks: []string{"never negative"}, Why: "the total goes below zero"},
	})
}

// Runs from TestPlantsTaggedBuild, which expects it to fail: a plant
// listing the wrong check, and one whose switch the sweep never
// reaches.
func TestPlantsTaggedMisnamed(t *testing.T) {
	if os.Getenv("DSTTEST_PLANTS_MUST_FAIL") == "" {
		t.Skip("meant to fail: TestPlantsTaggedBuild runs it")
	}
	dsttest.Plants(t, dsttest.Options{Seeds: 5, KeepGoing: true}, counter, []dsttest.Plant{
		{Bug: "lost-update", Checks: []string{"never negative"}},
		{Bug: "never-placed", Checks: []string{"never negative"}},
	})
}

// Runs from TestPlantsTaggedBuild, which expects its baseline to fail:
// the system is broken with no plant.
func TestPlantsTaggedBrokenBaseline(t *testing.T) {
	if os.Getenv("DSTTEST_PLANTS_MUST_FAIL") == "" {
		t.Skip("meant to fail: TestPlantsTaggedBuild runs it")
	}
	broken := func(h *dsttest.Harness) {
		counter(h)
		h.Final("broken anyway", func(w *dst.World) error { return errors.New("always") })
	}
	dsttest.Plants(t, dsttest.Options{Seeds: 3}, broken, nil)
}
