package gcounter_test

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/gcounter"
)

var names = []string{"n1", "n2", "n3"}

func addr(name string) string { return name + ":7000" }

func harness(bugs gcounter.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		for _, name := range names {
			var peers []string
			for _, p := range names {
				if p != name {
					peers = append(peers, addr(p))
				}
			}
			h.Machine(name, func(w *dst.World) {
				n, err := gcounter.Start(w, name, peers, bugs)
				// An I/O error reading the state is transient: retry, as a
				// service retries its startup. Anything else is a failure.
				for errors.Is(err, syscall.EIO) {
					h.Sometimes("reading the state failed")
					w.Sleep(50 * time.Millisecond)
					n, err = gcounter.Start(w, name, peers, bugs)
				}
				if err != nil {
					h.Failf("%s: %v", name, err)
					return
				}
				ln, err := w.Net().Listen("tcp", addr(name))
				if err != nil {
					h.Failf("listen: %v", err)
					return
				}
				w.Go(n.Gossip)
				n.Serve(ln)
			})
		}
		var acked, attempted uint64
		for range 3 {
			h.Go(func(w *dst.World) {
				for range 20 {
					attempted++
					if gcounter.Inc(w, addr(names[w.Rand().IntN(len(names))])) {
						acked++
					} else {
						h.Sometimes("increment outcome unknown")
					}
					w.Sleep(time.Duration(w.Rand().IntN(200)) * time.Millisecond)
				}
			})
		}
		h.Final("all nodes converge on the count", func(w *dst.World) error {
			var values []uint64
			for _, name := range names {
				var v uint64
				var err error
				d := 200 * time.Millisecond
				for range 10 { // the network is whole; retry resets, and wait longer on slow links
					if v, err = gcounter.Get(w, addr(name), d); err == nil {
						break
					}
					d = min(2*d, 5*time.Second)
				}
				if err != nil {
					return fmt.Errorf("get %s: %v", name, err)
				}
				values = append(values, v)
			}
			if slices.Min(values) != slices.Max(values) {
				return fmt.Errorf("nodes disagree: %v", values)
			}
			if v := values[0]; v < acked || v > attempted {
				return fmt.Errorf("count %d outside [%d acknowledged, %d attempted]", v, acked, attempted)
			}
			return nil
		})
		h.Expect("increment outcome unknown")
	}
}

var opts = dsttest.Options{
	Seeds:         200,
	Faults:        dsttest.AllFaults,
	FaultPhase:    5 * time.Second,
	RecoveryPhase: 3 * time.Second,
}

// Verifies: [converges], [counts-exactly]
// Fails if: nodes disagree after recovery, or the count loses acknowledged
// increments or invents ones never attempted.
func TestGCounter(t *testing.T) {
	dsttest.Run(t, opts, harness(gcounter.Bugs{}))
}

// Each planted bug must be caught as a wrong or unsettled count, or (a
// rename without fsync) as the truncated state file it leaves after a
// power loss.
func TestPlantedBugs(t *testing.T) {
	for name, bugs := range map[string]gcounter.Bugs{
		"MergeSum":            {MergeSum: true},
		"NoFsyncBeforeRename": {NoFsyncBeforeRename: true},
	} {
		t.Run(name, func(t *testing.T) {
			o := opts
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			err := rep.Failed[0].Err.Error()
			truncated := bugs.NoFsyncBeforeRename && strings.Contains(err, "corrupt state")
			if !strings.Contains(err, "outside") && !strings.Contains(err, "disagree") && !truncated {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, rep.Failed[0].Err)
		})
	}
}
