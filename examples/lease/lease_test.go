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

func harness(bugs lease.Bugs) func(h *dsttest.Harness) {
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
		clients := map[string]*lease.Client{}
		var machines []*dsttest.Machine
		for i := range 3 {
			name := fmt.Sprintf("c%d", i)
			machines = append(machines, h.Machine(name, func(w *dst.World) {
				c := lease.NewClient(w, name, "lock:1", bugs)
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

var opts = dsttest.Options{
	Seeds:         300,
	Faults:        dsttest.Faults{Crash: true, Partition: true, Network: true},
	FaultPhase:    5 * time.Second,
	RecoveryPhase: 2 * time.Second,
}

// Verifies: [one-holder], [lease-is-granted]
// Fails if: two clients ever believe they hold the lease at once, or no
// one gets it after faults stop.
func TestLease(t *testing.T) {
	dsttest.Run(t, opts, harness(lease.Bugs{}))
}

// Each planted bug must be caught as two simultaneous holders.
func TestPlantedBugs(t *testing.T) {
	for name, bugs := range map[string]lease.Bugs{
		"ClockFromReply": {ClockFromReply: true},
		"NoStepDown":     {NoStepDown: true},
		"ServerForgets":  {ServerForgets: true},
	} {
		t.Run(name, func(t *testing.T) {
			o := opts
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			if err := rep.Failed[0].Err.Error(); !strings.Contains(err, "all believe they hold it") {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, rep.Failed[0].Err)
		})
	}
}
