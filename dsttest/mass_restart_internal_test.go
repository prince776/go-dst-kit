package dsttest

import (
	"fmt"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Verifies: [mass-restart]
// Fails if: (draws, 300 seeds, with MassRestart alone, among every class,
// and without it) a seed's Swarm differs from its draw without mass
// restarts in any field but the four MassRestart ones; a seed without
// the fault has any of them set; a share falls outside [0.5, 1] or a
// spread outside [0, 10s]; (runs, 60 seeds of five crashable machines and
// a NeverCrash one, read from the machines' Downtime, not the nemesis's
// own counts) an event has fewer than ⌈share·k⌉ machines down within its
// spread; a machine goes down outside every event's window (other than
// by an ordinary crash, which Kill turns on too) or in the recovery phase; a machine isn't back by the end; the NeverCrash machine
// ever goes down; an event's machines are killed without Faults.Kill, or
// lose power with it alone; any event happens without the fault; or
// (against vacuity) mass restarts aren't drawn in about two thirds of the
// seeds, none takes every machine, none is instant, or all are; or in the
// runs fewer than 20 seeds have one, no instant has every crashable
// machine down, no event spreads its crashes over more than a second, or
// kills or power losses never happen.
func TestMassRestart(t *testing.T) {
	// Draws.
	const drawSeeds = 300
	mass := Faults{MassRestart: true}
	all := Faults{Crash: true, Kill: true, Partition: true, Network: true, Pause: true, Gray: true, Loss: true, DiskErrors: true, Disk: true, MassRestart: true}
	var on, whole, instant int
	for _, f := range []Faults{mass, all, {Crash: true, Partition: true, Gray: true}} {
		for seed := uint64(1); seed <= drawSeeds; seed++ {
			noMassRestart = true
			base := newSwarm(seed, f, time.Minute)
			noMassRestart = false
			got := newSwarm(seed, f, time.Minute)
			if !f.MassRestart {
				if !reflect.DeepEqual(got, base) {
					t.Fatalf("seed %d (%+v), no mass restarts: Swarm differs from its draw without them:\n got %+v\nbase %+v", seed, f, got, base)
				}
				continue
			}
			if got.MassRestartEvery == 0 {
				if got.MassRestartShare != 0 || got.MassRestartSpread != 0 || got.MassRestartDown != 0 {
					t.Fatalf("seed %d: no mass restarts, but %+v", seed, got)
				}
			} else {
				if got.MassRestartShare < 0.5 || got.MassRestartShare > 1 || got.MassRestartSpread < 0 || got.MassRestartSpread > 10*time.Second || got.MassRestartDown <= 0 {
					t.Fatalf("seed %d: mass restart fields out of range: %+v", seed, got)
				}
				if f == mass {
					on++
					if got.MassRestartShare == 1 {
						whole++
					}
					if got.MassRestartSpread == 0 {
						instant++
					}
				}
			}
			want := got
			want.MassRestartEvery, want.MassRestartShare, want.MassRestartSpread, want.MassRestartDown = 0, 0, 0, 0
			if !reflect.DeepEqual(want, base) {
				t.Fatalf("seed %d (%+v): a field besides the mass restart ones differs:\n got %+v\nbase %+v", seed, f, got, base)
			}
		}
	}
	if share := float64(on) / drawSeeds; share < 0.55 || share > 0.78 {
		t.Errorf("%d of %d seeds have mass restarts (%.2f), want about two thirds", on, drawSeeds, share)
	}
	if whole == 0 || instant == 0 || whole == on || instant == on {
		t.Errorf("of %d seeds with mass restarts, %d take every machine and %d are instant; want some of each, not all", on, whole, instant)
	}

	// Runs.
	crashable := []string{"a", "b", "c", "d", "e"}
	const faultPhase = 20 * time.Second
	var events, allDown, spreadOut, kills, losses, seedsWith int
	for seed := uint64(1); seed <= 60; seed++ {
		f := Faults{MassRestart: true, Kill: seed%2 == 0}
		opts := Options{Faults: f, FaultPhase: faultPhase, RecoveryPhase: time.Second}
		down := map[string][]span{}
		var sw Swarm
		var log []string
		res := runSeed(seed, opts, func(h *Harness) {
			for _, n := range append(crashable, "z") {
				var o []MachineOption
				if n == "z" {
					o = append(o, NeverCrash)
				}
				h.Machine(n, func(w *dst.World) {
					for {
						w.Sleep(10 * time.Millisecond)
					}
				}, o...)
			}
			h.Final("record downtime", func(*dst.World) error {
				sw = h.Swarm()
				base := h.World().Now().Add(-h.sim.S.Now())
				for _, m := range h.Machines() {
					for _, i := range m.Downtime() {
						s := span{from: i.From.Sub(base), open: i.To.IsZero()}
						if !s.open {
							s.to = i.To.Sub(base)
						}
						down[m.Name()] = append(down[m.Name()], s)
					}
				}
				return nil
			})
		}, func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) })
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		// From the log: when each event started (only that), the
		// ordinary crashes (Kill turns them on too; their own laws cover
		// them), and the kind of each down (the scheduler's own lines).
		var starts []time.Duration
		kind := map[string]string{} // machine@from: crash or kill
		ordinary := map[string]bool{}
		for _, l := range log {
			ts, ev, _ := strings.Cut(strings.TrimSpace(l), " ")
			at, err := time.ParseDuration(ts)
			if err != nil {
				continue
			}
			if strings.HasPrefix(ev, "nemesis: mass restart") {
				starts = append(starts, at)
			}
			if fs := strings.Fields(ev); len(fs) == 3 && (fs[1] == "crash" || fs[1] == "kill") && fs[0] == "nemesis:" {
				ordinary[fmt.Sprintf("%s@%v", fs[2], at)] = true
			}
			if fs := strings.Fields(ev); len(fs) >= 2 && (fs[0] == "crash" || fs[0] == "kill") {
				kind[fmt.Sprintf("%s@%v", fs[1], at)] = fs[0]
			}
		}
		if sw.MassRestartEvery == 0 && len(starts) > 0 {
			t.Fatalf("seed %d: %d mass restarts with MassRestartEvery 0", seed, len(starts))
		}
		if len(starts) > 0 {
			seedsWith++
		}
		if len(down["z"]) > 0 {
			t.Fatalf("seed %d: the NeverCrash machine went down: %v", seed, down["z"])
		}
		need := int(math.Ceil(sw.MassRestartShare * float64(len(crashable))))
		in := func(at, start time.Duration) bool { return at >= start && at <= start+sw.MassRestartSpread }
		for _, start := range starts {
			events++
			n := 0
			first, last := time.Duration(math.MaxInt64), time.Duration(0)
			for _, m := range crashable {
				hit := false
				for _, s := range down[m] {
					if s.from <= start+sw.MassRestartSpread && (s.open || s.to >= start) {
						hit = true
					}
					if in(s.from, start) {
						first, last = min(first, s.from), max(last, s.from)
					}
				}
				if hit {
					n++
				}
			}
			if n < need {
				t.Fatalf("seed %d: the mass restart at %v had %d of %d machines down within %v, want at least %d (share %.2f)", seed, start, n, len(crashable), sw.MassRestartSpread, need, sw.MassRestartShare)
			}
			if last-first > time.Second {
				spreadOut++
			}
		}
		for _, m := range crashable {
			for _, s := range down[m] {
				if s.open {
					t.Fatalf("seed %d: %s never came back (down from %v)", seed, m, s.from)
				}
				if ordinary[fmt.Sprintf("%s@%v", m, s.from)] {
					continue
				}
				if s.from >= faultPhase {
					t.Fatalf("seed %d: %s went down at %v, in the recovery phase", seed, m, s.from)
				}
				ok := false
				for _, start := range starts {
					ok = ok || in(s.from, start)
				}
				if !ok {
					t.Fatalf("seed %d: %s went down at %v, outside every mass restart (%v, spread %v)", seed, m, s.from, starts, sw.MassRestartSpread)
				}
				switch k := kind[fmt.Sprintf("%s@%v", m, s.from)]; {
				case k == "kill" && !f.Kill, k == "crash" && f.Kill:
					t.Fatalf("seed %d (Kill %v): %s went down by a %s at %v", seed, f.Kill, m, k, s.from)
				case k == "kill":
					kills++
				case k == "crash":
					losses++
				default:
					t.Fatalf("seed %d: no crash or kill logged for %s at %v", seed, m, s.from)
				}
				// Every crashable machine down at this instant?
				at := 0
				for _, o := range crashable {
					for _, x := range down[o] {
						if x.from <= s.from && x.to > s.from {
							at++
							break
						}
					}
				}
				if at == len(crashable) {
					allDown++
				}
			}
		}
	}
	if seedsWith < 20 || events == 0 || allDown == 0 || spreadOut == 0 || kills == 0 || losses == 0 {
		t.Fatalf("vacuous: %d seeds with mass restarts (%d events), %d instants with every machine down, %d events spread over more than 1s, %d kills, %d power losses",
			seedsWith, events, allDown, spreadOut, kills, losses)
	}
	t.Logf("%d of 60 seeds had mass restarts: %d events, %d instants with every machine down, %d spread over more than 1s, %d kills, %d power losses",
		seedsWith, events, allDown, spreadOut, kills, losses)

	// Without the fault, none.
	for seed := uint64(1); seed <= 10; seed++ {
		var log []string
		runSeed(seed, Options{Faults: Faults{Crash: true, Kill: true}, FaultPhase: 5 * time.Second, RecoveryPhase: time.Second}, func(h *Harness) {
			h.Machine("a", func(w *dst.World) {
				for {
					w.Sleep(10 * time.Millisecond)
				}
			})
		}, func(format string, args ...any) { log = append(log, fmt.Sprintf(format, args...)) })
		for _, l := range log {
			if strings.Contains(l, "nemesis: mass") {
				t.Fatalf("seed %d: a mass restart without the fault: %s", seed, l)
			}
		}
	}
}
