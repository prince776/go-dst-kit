package heartbeat_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/heartbeat"
)

var workers = []string{"w0", "w1", "w2"}

// leaveAfter is when each worker leaves (0: never).
var leaveAfter = map[string]time.Duration{"w0": 1500 * time.Millisecond, "w1": 3 * time.Second}

func harness(bugs heartbeat.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		now := func() time.Time { return h.World().Now() } // true time
		var mon *heartbeat.Monitor
		var monRunning time.Time // since when the monitor has run unpaused
		monM := h.Machine("monitor", func(w *dst.World) {
			m := heartbeat.NewMonitor(w, workers, bugs)
			m.OnStatus = func(name string, from, to heartbeat.Status) {
				if from == heartbeat.Leaving && to == heartbeat.Active {
					h.Failf("zombie: the monitor saw %s leave, then active again", name)
				}
				if to == heartbeat.Leaving {
					h.Sometimes("monitor saw a worker leave")
				}
			}
			mon, monRunning = m, now()
			pc, err := w.Net().ListenPacket("udp", "monitor:9")
			if err != nil {
				h.Failf("listen: %v", err)
				return
			}
			m.Serve(pc)
		})
		live := map[string]*heartbeat.Worker{}
		machines := map[string]*dsttest.Machine{}
		for _, name := range workers {
			machines[name] = h.Machine(name, func(w *dst.World) {
				wk := heartbeat.NewWorker(w, name, "monitor:9", bugs)
				live[name] = wk
				wk.Run(leaveAfter[name])
			})
		}
		// [complete]: a worker down for Timeout plus the worst network
		// delay is suspected by a monitor running unpaused that long (a
		// paused monitor reads stale heartbeats when it resumes).
		downSince := map[string]time.Time{}
		sw := h.Swarm()
		// The worst delay includes the link's added latency when it is slow
		// (Gray), which h.Link reports.
		bound := func(worker string) time.Duration {
			return heartbeat.Timeout + sw.MinLatency + 10*sw.MeanLatency + h.Link(worker, "monitor").Extra + 10*time.Millisecond
		}
		h.Always("a worker down long enough is suspected", func() error {
			t := now()
			if monM.Dead() || monM.Paused() {
				monRunning = time.Time{}
			} else if monRunning.IsZero() {
				monRunning = t
			}
			for _, name := range workers {
				m := machines[name]
				if !m.Dead() {
					delete(downSince, name)
					continue
				}
				if _, ok := downSince[name]; !ok {
					downSince[name] = t
				}
				if mon == nil || monRunning.IsZero() {
					continue
				}
				from := downSince[name]
				if monRunning.After(from) {
					from = monRunning
				}
				if t.Sub(from) > bound(name) {
					if !mon.Suspected(name) {
						return fmt.Errorf("%s has been down for %v, the monitor running for %v, and it isn't suspected", name, t.Sub(downSince[name]), t.Sub(monRunning))
					}
					h.Sometimes("a crashed worker suspected")
				}
			}
			return nil
		})
		// [eventually-accurate]
		h.Final("every live worker is trusted, with its status", func(w *dst.World) error {
			for _, name := range workers {
				wk := live[name]
				if mon.Suspected(name) {
					return fmt.Errorf("%s is up but suspected", name)
				}
				if got, want := mon.Status(name), wk.Status(); got != want {
					return fmt.Errorf("the monitor has %s as %q, but it is %q", name, got, want)
				}
			}
			return nil
		})
		h.Expect("monitor saw a worker leave", "a crashed worker suspected")
	}
}

func opts() dsttest.Options {
	return dsttest.Options{
		Seeds:         300,
		Faults:        dsttest.AllFaults,
		FaultPhase:    5 * time.Second,
		RecoveryPhase: 2 * time.Second,
	}
}

// Verifies: [no-zombies], [complete], [eventually-accurate]
// Fails if: the correct detector breaks an invariant on any seed (the
// planted bugs below show each invariant can fail).
func TestHeartbeat(t *testing.T) {
	dsttest.Run(t, opts(), harness(heartbeat.Bugs{}))
}

// Each planted bug must be caught, by the invariant it breaks.
func TestPlantedBugs(t *testing.T) {
	for name, c := range map[string]struct {
		bugs heartbeat.Bugs
		want string
	}{
		"NoSeq":         {heartbeat.Bugs{NoSeq: true}, "zombie"},
		"NoIncarnation": {heartbeat.Bugs{NoIncarnation: true}, "is up but suspected"},
		"LearnsMembers": {heartbeat.Bugs{LearnsMembers: true}, "isn't suspected"},
	} {
		t.Run(name, func(t *testing.T) {
			o := opts()
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(c.bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%s not caught in %d seeds", name, rep.Runs)
			}
			if err := rep.Failed[0].Err.Error(); !strings.Contains(err, c.want) {
				t.Fatalf("caught for the wrong reason: %s", err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, rep.Failed[0].Err)
		})
	}
}
