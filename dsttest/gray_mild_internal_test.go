package dsttest

import (
	"reflect"
	"testing"
	"time"
)

// grayKinds are the gray kinds' on/off shares, in a Swarm.
func grayKinds(s *Swarm) []*float64 {
	return []*float64{&s.SlowLinkShare, &s.LossyLinkShare, &s.HealResetProb, &s.SlowMachineShare, &s.SlowDiskShare, &s.UDPBlockShare}
}

func grayOn(s Swarm) int {
	on := 0
	for _, k := range grayKinds(&s) {
		if *k > 0 {
			on++
		}
	}
	return on
}

// Verifies: [gray-mild]
// Fails if: over 300 seeds, with Gray (alone and among every class) or
// without it, a seed's Swarm differs from its draw without mildness in
// any field but the dropped gray kinds and MildGray; a seed not marked
// mild differs at all; a mild seed keeps more than two gray kinds on,
// keeps none when some would be on, or turns on one that was off; a seed
// without Gray is marked mild; or (against vacuity) mild seeds aren't
// about a third of the Gray ones, or none keeps one or none keeps two.
func TestGrayMild(t *testing.T) {
	const seeds = 300
	gray := Faults{Gray: true}
	all := Faults{Crash: true, Kill: true, Partition: true, Network: true, Pause: true, Gray: true, Loss: true, DiskErrors: true}
	mild, kept1, kept2 := 0, 0, 0
	for _, f := range []Faults{gray, all, {Crash: true, Partition: true}} {
		for seed := uint64(1); seed <= seeds; seed++ {
			noMildGray = true
			base := newSwarm(seed, f, time.Minute)
			noMildGray = false
			got := newSwarm(seed, f, time.Minute)
			if !got.MildGray {
				if !reflect.DeepEqual(got, base) {
					t.Fatalf("seed %d (%+v), not mild: Swarm differs from its draw without mildness:\n got %+v\nbase %+v", seed, f, got, base)
				}
				continue
			}
			if !f.Gray {
				t.Fatalf("seed %d: marked mild without Gray", seed)
			}
			// Only the dropped gray kinds (and the mark) may differ.
			want := base
			want.MildGray = true
			gk, wk := grayKinds(&got), grayKinds(&want)
			for i := range gk {
				switch {
				case *gk[i] == *wk[i]:
				case *gk[i] == 0:
					*wk[i] = 0 // dropped
				default:
					t.Fatalf("seed %d: gray kind %d changed from %v to %v, not dropped", seed, i, *wk[i], *gk[i])
				}
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("seed %d, mild: a field besides the dropped gray kinds differs:\n got %+v\nbase %+v", seed, got, base)
			}
			on, wouldBe := grayOn(got), grayOn(base)
			if on > 2 || wouldBe > 0 && on == 0 || on > wouldBe {
				t.Fatalf("seed %d, mild: %d gray kinds on, %d would be", seed, on, wouldBe)
			}
			if f == gray {
				mild++
				if on == 1 && wouldBe > 1 {
					kept1++
				}
				if on == 2 {
					kept2++
				}
			}
		}
	}
	if share := float64(mild) / seeds; share < 0.25 || share > 0.42 {
		t.Errorf("%d of %d Gray seeds mild (%.2f), want about a third", mild, seeds, share)
	}
	if kept1 == 0 || kept2 == 0 {
		t.Errorf("mild seeds keeping one kind of several: %d, keeping two: %d; want both", kept1, kept2)
	}
	t.Logf("%d of %d Gray seeds mild: %d kept one kind (of several), %d kept two", mild, seeds, kept1, kept2)
}
