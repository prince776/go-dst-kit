package dsttest_test

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsttest"
)

// panicky is a machine that writes a file unsynced, then panics with msg
// in its first incarnation while a sibling goroutine counts ticks; later
// incarnations just count. It reports what the test checks.
type panicky struct {
	boots       int
	ticksAfter  int  // ticks counted by the first incarnation after its panic
	sawUnsynced bool // the second boot read the first's unsynced write
}

func (p *panicky) setup(h *dsttest.Harness, msg string, isFailStop func(any) bool, opts ...dsttest.MachineOption) {
	if isFailStop != nil {
		h.OnPanic(isFailStop)
	}
	panicked := false
	h.Machine("svc", func(w *dst.World) {
		p.boots++
		boot := p.boots
		if boot == 2 {
			b, err := dst.ReadFile(w.FS(), "f")
			p.sawUnsynced = err == nil && string(b) == "unsynced"
		}
		w.Go(func() {
			for {
				w.Sleep(10 * time.Millisecond)
				if boot == 1 && panicked {
					p.ticksAfter++
				}
			}
		})
		if boot == 1 {
			f, _ := w.FS().OpenFile("f", os.O_CREATE|os.O_WRONLY, 0o644)
			f.Write([]byte("unsynced"))
			w.Sleep(50 * time.Millisecond)
			panicked = true
			panic(msg)
		}
	}, opts...)
}

// Verifies: [panic-fail-stop]
// Fails if: a panic OnPanic accepts fails the run, leaves its machine's
// other goroutines running, loses the disk's unsynced writes (a process
// death keeps the page cache), or isn't followed by a restart; or if a
// panic it rejects, a panic with no OnPanic, or one on a NeverCrash
// machine doesn't fail the run (the negatives).
func TestPanicFailStop(t *testing.T) {
	opts := dsttest.Options{Seeds: 1, FaultPhase: time.Second, RecoveryPhase: time.Second, DeterminismChecks: -1}
	failStop := func(v any) bool { return strings.Contains(fmt.Sprint(v), "disk died") }

	var p panicky
	rep := dsttest.Sweep(opts, func(h *dsttest.Harness) { p = panicky{}; p.setup(h, "disk died", failStop) })
	if !rep.OK() {
		t.Fatalf("an accepted panic failed the run: %v", rep.Failed[0].Err)
	}
	if p.boots < 2 {
		t.Errorf("the machine wasn't restarted after its process died (%d boots)", p.boots)
	}
	if p.ticksAfter > 0 {
		t.Errorf("the dead process's other goroutine kept running (%d ticks)", p.ticksAfter)
	}
	if !p.sawUnsynced {
		t.Errorf("the restarted process didn't see its predecessor's unsynced write: a panic must keep the page cache")
	}

	for _, c := range []struct {
		name       string
		msg        string
		isFailStop func(any) bool
		opts       []dsttest.MachineOption
	}{
		{"rejected", "nil map", failStop, nil},
		{"no OnPanic", "disk died", nil, nil},
		{"NeverCrash", "disk died", failStop, []dsttest.MachineOption{dsttest.NeverCrash}},
	} {
		var p panicky
		rep := dsttest.Sweep(opts, func(h *dsttest.Harness) { p = panicky{}; p.setup(h, c.msg, c.isFailStop, c.opts...) })
		var pe interface{ Error() string }
		if rep.OK() || !errors.As(rep.Failed[0].Err, &pe) || !strings.Contains(rep.Failed[0].Err.Error(), "panicked: "+c.msg) {
			t.Errorf("%s: the panic didn't fail the run: %+v", c.name, rep.Failed)
		}
	}
}
