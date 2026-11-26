package lease_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/lease"
)

// Drift bounds the tests use: realistic, and exaggerated to reach the
// edge of the drift assumption quickly.
const (
	realistic   = 100e-6
	exaggerated = 0.05
)

func harness(bugs lease.Bugs, drift float64) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		var lastHolder string
		var grantsInRecovery int
		h.Machine("lock", func(w *dst.World) {
			s := lease.NewServer(w, bugs)
			s.OnGrant = func(client string) {
				if client != lastHolder && lastHolder != "" {
					h.Sometimes("lease handed over")
				}
				lastHolder = client
				if h.Phase() == dsttest.Recovery {
					grantsInRecovery++
				}
			}
			ln, err := w.Net().Listen("tcp", "lock:1")
			if err != nil {
				h.Failf("listen: %v", err)
				return
			}
			s.Serve(ln)
		})
		// The store plays a reliable storage service: never crashed (it can
		// still be paused and partitioned).
		var store *lease.Store
		h.Machine("store", func(w *dst.World) {
			store = lease.NewStore(w, bugs)
			ln, err := w.Net().Listen("tcp", "store:2")
			if err != nil {
				h.Failf("listen: %v", err)
				return
			}
			store.Serve(ln)
		}, dsttest.NeverCrash)
		h.Always("the store never accepts a stale holder's write", func() error {
			if store != nil && store.Regressed != "" {
				return fmt.Errorf("%s", store.Regressed)
			}
			return nil
		})
		clients := map[string]*lease.Client{}
		var machines []*dsttest.Machine
		for i := range 3 {
			name := fmt.Sprintf("c%d", i)
			machines = append(machines, h.Machine(name, func(w *dst.World) {
				c := lease.NewClient(w, name, "lock:1", bugs)
				c.MaxDrift = drift
				c.Store = "store:2"
				clients[name] = c // a restarted client starts with no lease
				c.Run()
			}))
		}
		h.Always("at most one live client believes it holds the lease", func() error {
			var holders []string
			for _, m := range machines {
				if c := clients[m.Name()]; c != nil && !m.Dead() && c.Holds() {
					holders = append(holders, m.Name())
				}
			}
			if len(holders) > 1 {
				return fmt.Errorf("%v all believe they hold it", holders)
			}
			return nil
		})
		h.Final("the lease is granted once faults stop", func(*dst.World) error {
			if grantsInRecovery == 0 {
				return fmt.Errorf("no grant during the recovery phase")
			}
			return nil
		})
		h.Expect("lease handed over")
	}
}

// optsFor tests right up to the drift bound the clients assume.
func optsFor(drift float64) dsttest.Options {
	return dsttest.Options{
		Seeds:         300,
		Faults:        dsttest.Faults{Crash: true, Partition: true, Network: true, Pause: true},
		FaultPhase:    5 * time.Second,
		RecoveryPhase: 2 * time.Second,
		Clock:         dsttest.ClockOptions{MaxDrift: drift},
	}
}

// Verifies: [one-holder], [lease-is-granted], [writes-are-fenced]
// Fails if: two clients ever believe they hold the lease at once, or no
// one gets it after faults stop.
func TestLease(t *testing.T) {
	for _, drift := range []float64{realistic, exaggerated} {
		t.Run(fmt.Sprintf("drift=%g", drift), func(t *testing.T) {
			dsttest.Run(t, optsFor(drift), harness(lease.Bugs{}, drift))
		})
	}
}

// Each planted bug must be caught: as two simultaneous holders, or as a
// stale holder's write getting into the store.
func TestPlantedBugs(t *testing.T) {
	for name, c := range map[string]struct {
		bugs  lease.Bugs
		drift float64 // the bound that exposes it
	}{
		"ClockFromReply": {lease.Bugs{ClockFromReply: true}, realistic},
		"NoStepDown":     {lease.Bugs{NoStepDown: true}, realistic},
		"ServerForgets":  {lease.Bugs{ServerForgets: true}, realistic},
		"NoDriftMargin":  {lease.Bugs{NoDriftMargin: true}, exaggerated},
		"AbsoluteExpiry": {lease.Bugs{AbsoluteExpiry: true}, realistic},
		"NoFencing":      {lease.Bugs{NoFencing: true}, realistic},
	} {
		t.Run(name, func(t *testing.T) {
			o := optsFor(c.drift)
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(c.bugs, c.drift))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			if err := rep.Failed[0].Err.Error(); !strings.Contains(err, "all believe they hold it") && !strings.Contains(err, "stale holder's write") {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, rep.Failed[0].Err)
		})
	}
}

// The clock bugs need skew: with synchronized clocks they pass.
func TestClockBugsNeedSkew(t *testing.T) {
	for name, bugs := range map[string]lease.Bugs{
		"NoDriftMargin":  {NoDriftMargin: true},
		"AbsoluteExpiry": {AbsoluteExpiry: true},
	} {
		o := optsFor(exaggerated)
		o.DeterminismChecks = -1
		o.Clock = dsttest.ClockOptions{Synchronized: true}
		if rep := dsttest.Sweep(o, harness(bugs, exaggerated)); len(rep.Failed) > 0 {
			t.Fatalf("%s failed with synchronized clocks: %v", name, rep.Failed[0].Err)
		}
	}
}
