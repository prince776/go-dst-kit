package workerpool_test

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
	"github.com/prince776/go-dst-kit/examples/workerpool"
	"github.com/prince776/go-dst-kit/realworld"
)

// work doubles its input after a random delay, failing sometimes.
func work(w *dst.World, failRate int) func(context.Context, int) (int, error) {
	return func(ctx context.Context, x int) (int, error) {
		w.Sleep(time.Duration(w.Rand().IntN(5)) * time.Millisecond)
		if failRate > 0 && w.Rand().IntN(failRate) == 0 {
			return 0, fmt.Errorf("input %d failed", x)
		}
		return 2 * x, nil
	}
}

func harness(bugs workerpool.Bugs) func(h *dsttest.Harness) {
	return func(h *dsttest.Harness) {
		for c := range 3 {
			h.Go(func(w *dst.World) {
				for range 5 {
					n := 1 + w.Rand().IntN(20)
					inputs := make([]int, n)
					for i := range inputs {
						inputs[i] = 100*c + i
					}
					failRate := []int{0, 10, 50}[w.Rand().IntN(3)]
					out, err := workerpool.Map(w, context.Background(), inputs, 1+w.Rand().IntN(4), work(w, failRate), bugs)
					if err != nil {
						if !strings.HasSuffix(err.Error(), "failed") {
							h.Failf("unexpected error %v", err)
						}
						h.Sometimes("map failed")
						continue
					}
					if len(out) != n {
						h.Failf("%d results for %d inputs", len(out), n)
						return
					}
					for i, v := range out {
						if v != 2*inputs[i] {
							h.Failf("result %d is %d, want %d", i, v, 2*inputs[i])
							return
						}
					}
					h.Sometimes("map succeeded")
				}
			})
		}
		h.Expect("map failed", "map succeeded")
	}
}

var opts = dsttest.Options{Seeds: 300, FaultPhase: time.Second, RecoveryPhase: time.Second}

// Verifies: [map-is-correct], [no-leaks]
// Fails if: a result is wrong or missing, an error is invented, or a
// goroutine Map started is still running at the end.
func TestMap(t *testing.T) {
	dsttest.Run(t, opts, harness(workerpool.Bugs{}))
}

// Each planted bug must be caught.
func TestPlantedBugs(t *testing.T) {
	for name, c := range map[string]struct {
		bugs workerpool.Bugs
		want []string // any of these symptoms
	}{
		"AddInWorker":   {workerpool.Bugs{AddInWorker: true}, []string{"want", "send on closed channel"}},
		"ReturnOnError": {workerpool.Bugs{ReturnOnError: true}, []string{"never finished"}},
		"CloseEarly":    {workerpool.Bugs{CloseEarly: true}, []string{"want", "send on closed channel"}},
	} {
		t.Run(name, func(t *testing.T) {
			o := opts
			o.DeterminismChecks = -1
			rep := dsttest.Sweep(o, harness(c.bugs))
			if len(rep.Failed) == 0 {
				t.Fatalf("%d seeds never caught %s", rep.Runs, name)
			}
			err := rep.Failed[0].Err
			if !slices.ContainsFunc(c.want, func(s string) bool { return strings.Contains(err.Error(), s) }) {
				t.Fatalf("%s caught for the wrong reason: %v", name, err)
			}
			t.Logf("caught at seed %d: %v", rep.Failed[0].Seed, firstLines(err, 3))
		})
	}
}

func firstLines(err error, n int) string {
	lines := strings.SplitN(err.Error(), "\n", n+1)
	return strings.Join(lines[:min(n, len(lines))], "\n")
}

// Verifies: [map-is-correct] in the real World: the same code runs on
// real goroutines and channels.
func TestReal(t *testing.T) {
	w := realworld.New(realworld.Options{})
	inputs := make([]int, 100)
	for i := range inputs {
		inputs[i] = i
	}
	out, err := workerpool.Map(w, context.Background(), inputs, 8, work(w, 0), workerpool.Bugs{})
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range out {
		if v != 2*i {
			t.Fatalf("out[%d] = %d", i, v)
		}
	}
	_, err = workerpool.Map(w, context.Background(), inputs, 8, func(ctx context.Context, x int) (int, error) {
		if x == 50 {
			return 0, errors.New("boom")
		}
		return x, nil
	}, workerpool.Bugs{})
	if err == nil || err.Error() != "boom" {
		t.Fatalf("err = %v", err)
	}
}
