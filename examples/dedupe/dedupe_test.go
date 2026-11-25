package dedupe_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/dedupe"
	"github.com/prince776/go-dst-kit/examples/wal"
)

const messages = 30

func harness(bugs dedupe.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		consumer := h.Machine("consumer", func(w *dst.World) {
			c, err := dedupe.NewConsumer(w, bugs)
			if err != nil {
				h.Failf("recover: %v", err)
				return
			}
			ln, err := w.Net().Listen("tcp", "consumer:9000")
			if err != nil {
				h.Failf("listen: %v", err)
				return
			}
			c.Serve(ln)
		})
		// Two producers deliver alternate messages concurrently.
		acked := map[string]bool{}
		for p := range 2 {
			h.Go(func(w *dst.World) {
				for i := p; i < messages; i += 2 {
					id := fmt.Sprintf("m%d", i)
					dedupe.Deliver(w, "consumer:9000", id)
					acked[id] = true
					w.Sleep(time.Duration(w.Rand().IntN(50)) * time.Millisecond)
				}
			})
		}
		h.Final("every message applied exactly once", func(w *dst.World) error {
			consumer.Crash() // whatever is applied must be durable
			recs, err := wal.Read(consumer.World().FS(), dedupe.EffectsLog)
			if err != nil {
				return err
			}
			count := map[string]int{}
			for _, r := range recs {
				count[string(r)]++
			}
			for i := range messages {
				id := fmt.Sprintf("m%d", i)
				switch n := count[id]; {
				case n > 1:
					return fmt.Errorf("%s applied %d times", id, n)
				case n == 0 && acked[id]:
					return fmt.Errorf("%s was acknowledged but never applied", id)
				case n == 0:
					return fmt.Errorf("%s was never delivered", id)
				}
			}
			return nil
		})
	}
}

var opts = dsttest.Options{
	Seeds:         200,
	Faults:        dsttest.AllFaults,
	FaultPhase:    5 * time.Second,
	RecoveryPhase: 5 * time.Second,
}

// Verifies: [exactly-once], [eventually-delivered]
// Fails if: a message is applied twice or lost, or delivery never finishes
// after faults stop (the workload would be stuck).
func TestExactlyOnce(t *testing.T) {
	dsttest.Run(t, opts, harness(dedupe.Bugs{}))
}

// Each planted bug must be caught, as a duplicate.
func TestPlantedBugs(t *testing.T) {
	for name, bugs := range map[string]dedupe.Bugs{
		"InMemorySeen":     {InMemorySeen: true},
		"SeparateSeenFile": {SeparateSeenFile: true},
	} {
		t.Run(name, func(t *testing.T) {
			o := opts
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			if err := rep.Failed[0].Err.Error(); !strings.Contains(err, "applied 2 times") {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, rep.Failed[0].Err)
		})
	}
}
