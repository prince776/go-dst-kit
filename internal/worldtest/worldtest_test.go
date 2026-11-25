package worldtest_test

import (
	"fmt"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/internal/worldtest"
	"github.com/prince776/go-dst-kit/realworld"
)

// Verifies: [same-code-both-worlds], [real-is-plain-go], [select-picks-ready], [timers-follow-world-clock]
// Fails if: either World differs from Go's semantics in any checked way.
func TestContractReal(t *testing.T) {
	for _, c := range worldtest.Checks {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			if err := runReal(c.Fn); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func runReal(fn func(*dst.World)) (err error) {
	done := make(chan error)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				done <- fmt.Errorf("%v", r)
			}
		}()
		fn(realworld.New(realworld.Options{}))
		done <- nil
	}()
	return <-done
}

func TestContractSim(t *testing.T) {
	for _, c := range worldtest.Checks {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			for seed := uint64(1); seed <= 50; seed++ {
				if r := simworld.Run(simworld.Options{Seed: seed}, c.Fn); r.Err != nil {
					t.Fatalf("seed %d: %v", seed, r.Err)
				}
			}
		})
	}
}

// The suite must be able to fail: a World that breaks a law fails a check.
// Here, a "World" whose channels lose FIFO order (simulated by a check run
// against deliberately wrong code) must be caught.
func TestSuiteCanFail(t *testing.T) {
	bad := func(w *dst.World) {
		// Wrong: receives in the wrong order would be accepted only if
		// bufferedFIFO didn't check order. Run the real check on a channel
		// we've reordered behind its back.
		c := dst.MakeChan[int](w, 3)
		c.Send(1)
		c.Send(0)
		c.Send(2)
		for i := range 3 {
			if v, _ := c.Recv(); v != i {
				panic(worldtest.Failure(fmt.Sprintf("recv %d, want %d", v, i)))
			}
		}
	}
	if r := simworld.Run(simworld.Options{Seed: 1}, bad); r.Err == nil {
		t.Fatal("an out-of-order receive was not reported in the simulated World")
	}
	if err := runReal(bad); err == nil {
		t.Fatal("an out-of-order receive was not reported in the real World")
	}
}

// Verifies: [behaves-like-os]
// Fails if: the simulated disk differs from the operating system's
// filesystem in any checked way (or the real World's FS isn't os).
func TestFSContract(t *testing.T) {
	for _, c := range worldtest.FSChecks {
		t.Run(c.Name+"/real", func(t *testing.T) {
			done := make(chan error)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						done <- fmt.Errorf("%v", r)
					}
				}()
				c.Fn(realworld.New(realworld.Options{Dir: t.TempDir()}))
				done <- nil
			}()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		})
		t.Run(c.Name+"/sim", func(t *testing.T) {
			for seed := uint64(1); seed <= 5; seed++ {
				if r := simworld.Run(simworld.Options{Seed: seed}, c.Fn); r.Err != nil {
					t.Fatalf("seed %d: %v", seed, r.Err)
				}
			}
		})
	}
}
