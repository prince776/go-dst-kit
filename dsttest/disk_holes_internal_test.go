package dsttest

import (
	"reflect"
	"testing"
	"time"
)

// Verifies: [disk-holes]
// Fails if: over 300 seeds and four fault sets, a seed's Swarm differs
// from its draw without holes in any field but DiskHoleProb; a seed
// without Disk faults gets a hole chance; or (against vacuity) the share
// of Disk seeds with holes isn't about two thirds, or the chances drawn
// don't spread over [0, 1).
func TestDiskHoles(t *testing.T) {
	const seeds = 300
	all := Faults{Crash: true, Kill: true, Partition: true, Disk: true, Network: true, Pause: true, Gray: true, Loss: true, DiskErrors: true}
	on, lo, hi := 0, 0, 0
	for _, f := range []Faults{{Disk: true}, {Crash: true, Disk: true}, all, {Crash: true, Partition: true}} {
		for seed := uint64(1); seed <= seeds; seed++ {
			noDiskHoles = true
			base := newSwarm(seed, f, time.Minute)
			noDiskHoles = false
			got := newSwarm(seed, f, time.Minute)
			p := got.DiskHoleProb
			if !f.Disk && p != 0 {
				t.Fatalf("seed %d (%+v): hole chance %v without Disk faults", seed, f, p)
			}
			got.DiskHoleProb = 0
			if !reflect.DeepEqual(got, base) {
				t.Fatalf("seed %d (%+v): Swarm differs from its draw without holes in more than DiskHoleProb:\n got %+v\nbase %+v", seed, f, got, base)
			}
			if f == (Faults{Disk: true}) && p > 0 {
				on++
				if p < 0.5 {
					lo++
				} else {
					hi++
				}
			}
		}
	}
	if share := float64(on) / seeds; share < 0.55 || share > 0.78 {
		t.Errorf("%d of %d Disk seeds have holes (%.2f), want about two thirds", on, seeds, share)
	}
	if lo == 0 || hi == 0 {
		t.Errorf("hole chances below 0.5: %d, above: %d; want both", lo, hi)
	}
	t.Logf("%d of %d Disk seeds have holes (%d below 0.5, %d above)", on, seeds, lo, hi)
}
