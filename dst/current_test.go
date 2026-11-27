package dst_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// currentRun runs one simulation with two machines; goroutines on each
// (started directly and by another goroutine) and a raw goroutine report
// what Current returns. It returns the first discrepancy.
func currentRun(seed uint64) error {
	sim := simworld.New(simworld.Options{Seed: seed})
	a := sim.AddMachine("a", simworld.MachineOptions{})
	b := sim.AddMachine("b", simworld.MachineOptions{})
	var bad error
	note := func(format string, args ...any) {
		if bad == nil {
			bad = fmt.Errorf("seed %d: "+format, append([]any{seed}, args...)...)
		}
	}
	ran := 0
	start := func(m *simworld.Machine) {
		w := m.World
		w.Go(func() {
			defer func() { ran++ }()
			if dst.Current() != m.World {
				note("a goroutine on %s got another World", m.Name)
			}
			var wg dst.WaitGroup
			wg.Go(w, func() {
				w.Sleep(time.Millisecond)
				if dst.Current() != m.World {
					note("a goroutine started on %s by another got another World", m.Name)
				}
			})
			raw := make(chan *dst.World)
			go func() { raw <- dst.Current() }() // a goroutine no World started
			got := <-raw                         //dst:ok the raw goroutine does no World operation
			if got.Simulated() {
				note("a raw goroutine got a simulated World")
			}
			wg.Wait()
		})
	}
	res := sim.Run(func(w *dst.World) {
		if dst.Current() != w {
			note("main got another World")
		}
		start(a)
		start(b)
		w.Sleep(10 * time.Millisecond)
	})
	if res.Err != nil {
		return res.Err
	}
	if bad == nil && ran != 2 {
		return fmt.Errorf("seed %d: %d of 2 machines' goroutines ran to the end", seed, ran)
	}
	return bad
}

// Verifies: [current-is-the-callers]
// Fails if: a goroutine of a simulated machine (started directly or by
// another of its goroutines, and with 8 simulations running at once) gets
// a World other than its machine's, a raw goroutine gets a simulated
// World, or Current outside any simulation isn't the real World (not
// simulated, real clock, usable randomness).
func TestCurrentIsTheCallers(t *testing.T) {
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for seed := uint64(i*10 + 1); seed <= uint64(i*10+10); seed++ {
				if err := currentRun(seed); err != nil {
					errs[i] = err
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	w := dst.Current()
	if w.Simulated() {
		t.Fatal("outside a simulation, Current is simulated")
	}
	if d := time.Since(w.Now()); d < 0 || d > time.Second {
		t.Fatalf("outside a simulation, Current's clock is %v off the real one", d)
	}
	_ = w.Rand().IntN(10)
}
