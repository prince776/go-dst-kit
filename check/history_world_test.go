package check_test

import (
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/check"
	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// Verifies: [times-are-the-worlds]
// Fails if: history times don't follow the simulated clock.
func TestHistoryUsesWorldClock(t *testing.T) {
	var ops []check.Op[check.KVInput, check.KVOutput]
	simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		h := check.NewHistory[check.KVInput, check.KVOutput](w)
		w.Sleep(time.Hour)
		c := h.Invoke(0, check.KVInput{Op: check.KVPut, Key: "k", Value: "v"})
		w.Sleep(2 * time.Second)
		c.Return(check.KVOutput{})
		ops = h.Ops()
	})
	if len(ops) != 1 || ops[0].Call != time.Hour || ops[0].Return != time.Hour+2*time.Second {
		t.Fatalf("ops %+v", ops)
	}
}
