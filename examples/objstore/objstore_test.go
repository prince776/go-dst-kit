package objstore_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/objstore"
)

func harness(bugs objstore.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		svc := objstore.NewService() // the cloud service: outside every machine
		outcomes := map[string]objstore.Outcome{}
		for _, name := range []string{"c0", "c1", "c2"} {
			boots := 0
			h.Machine(name, func(w *dst.World) {
				boots++
				log := objstore.NewLog(w, svc.Bucket(w), "log", bugs)
				for i := 0; ; i++ {
					id := fmt.Sprintf("%s.%d.%d", name, boots, i)
					o := log.Append(id)
					outcomes[id] = o
					if o == objstore.Unknown {
						h.Sometimes("an append's outcome is unknown")
					}
					w.Sleep(20 * time.Millisecond) // every append rereads the whole log
				}
			})
		}
		h.Final("the log agrees with what clients were told", func(w *dst.World) error {
			entries := objstore.Entries(svc.Peek("log"))
			in := map[string]int{}
			for _, id := range entries {
				in[id]++
				if in[id] == 2 {
					return fmt.Errorf("duplicate: %s is in the log twice", id)
				}
			}
			ids := slices.Sorted(func(yield func(string) bool) {
				for id := range outcomes {
					if !yield(id) {
						return
					}
				}
			})
			for _, id := range ids {
				switch o := outcomes[id]; {
				case o == objstore.Applied && in[id] == 0:
					return fmt.Errorf("lost update: %s was reported applied but isn't in the log", id)
				case o == objstore.NotApplied && in[id] > 0:
					return fmt.Errorf("lost ack: %s was reported not applied but is in the log", id)
				}
			}
			if len(entries) == 0 {
				return fmt.Errorf("nothing was appended")
			}
			return nil
		})
		h.Expect("point objstore.put: fail-after", "service outage")
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

// Verifies: [no-duplicates], [applied-is-durable], [not-applied-is-true]
// Fails if: the correct client breaks any of them on some seed (each
// planted bug below breaks one, so each check can fail).
func TestObjstore(t *testing.T) {
	dsttest.Run(t, opts(), harness(objstore.Bugs{}))
}

// Each planted bug must be caught, by the check it breaks.
func TestPlantedBugs(t *testing.T) {
	for name, c := range map[string]struct {
		bugs objstore.Bugs
		want string
	}{
		"BlindRetry":       {objstore.Bugs{BlindRetry: true}, "duplicate"},
		"TimeoutAsFailure": {objstore.Bugs{TimeoutAsFailure: true}, "lost ack"},
		"NoConditional":    {objstore.Bugs{NoConditional: true}, "lost update"},
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

// The ambiguity bugs only show when a write can time out after taking
// effect: without fault points they pass every seed.
func TestAmbiguityBugsNeedFaultPoints(t *testing.T) {
	o := opts()
	o.Seeds, o.DeterminismChecks = 100, -1
	o.Faults.Points, o.Faults.Services = false, false
	for name, bugs := range map[string]objstore.Bugs{
		"BlindRetry":       {BlindRetry: true},
		"TimeoutAsFailure": {TimeoutAsFailure: true},
	} {
		rep := dsttest.Sweep(o, harness(bugs)) // (its coverage expectations go unmet: no points)
		if len(rep.Failed) > 0 {
			t.Errorf("%s caught without fault points: %v", name, rep.Failed[0].Err)
		}
	}
}
