package sched_test

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// next is the code site of the caller's next line, as the scheduler
// names sites: function:line.
func nextLine() string {
	pc, _, line, _ := runtime.Caller(1)
	return runtime.FuncForPC(pc).Name() + ":" + strconv.Itoa(line+1)
}

// pingPong is two goroutines handing values over an unbuffered channel,
// and main waiting for them, each at a line it records in sites; names
// gets the goroutines' task names (their start sites).
func pingPong(sim *simworld.Sim, sites map[string]bool, names map[string]bool) func(w *dst.World) {
	return func(w *dst.World) {
		names[sim.S.Current().Name] = true
		c := dst.MakeChan[int](w, 0)
		done := dst.MakeChan[bool](w, 2)
		sites[nextLine()] = true // w.Go is a scheduling point too
		w.Go(func() {
			names[sim.S.Current().Name] = true
			for i := range 20 {
				sites[nextLine()] = true
				c.Send(i)
			}
			sites[nextLine()] = true // so is a send that doesn't block
			done.Send(true)
		})
		sites[nextLine()] = true // w.Go is a scheduling point too
		w.Go(func() {
			names[sim.S.Current().Name] = true
			for range 20 {
				sites[nextLine()] = true
				c.Recv()
			}
			sites[nextLine()] = true // so is a send that doesn't block
			done.Send(true)
		})
		for range 2 {
			sites[nextLine()] = true
			done.Recv()
		}
	}
}

// runPairs runs pingPong on seed with pairs recorded or not.
func runPairs(seed uint64, pairs bool) (simworld.Result, map[uint64]uint64, map[string]bool, map[string]bool) {
	sites, names := map[string]bool{}, map[string]bool{}
	sim := simworld.New(simworld.Options{Seed: seed, Pairs: pairs})
	r := sim.Run(pingPong(sim, sites, names))
	return r, sim.S.FirstPairs(), sites, names
}

// Verifies: [pairs-name-program-lines]
// Fails if: a recorded pair has a site that isn't a line of the program
// under test or a goroutine's start (a go-dst-kit frame leaked in, or the
// library fallback was used), or the sender-then-receiver handoff is never
// recorded, or recording pairs changes the run, or records anything when
// off.
func TestPairs(t *testing.T) {
	sawHandoff := false
	for seed := uint64(1); seed <= 20; seed++ {
		r, pairs, sites, names := runPairs(seed, true)
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		if len(pairs) == 0 {
			t.Fatalf("seed %d: no pairs recorded", seed)
		}
		// From: a handoff site, a finished task, or nothing yet (the run's
		// first step).
		from := []uint64{sched.SiteFinished, 0}
		var to []uint64
		var send, recv uint64
		for s := range sites {
			h := sched.HashSite(s)
			from, to = append(from, h), append(to, h)
			switch fn, _, _ := strings.Cut(s, ":"); {
			case strings.HasSuffix(fn, "pingPong.func1.1"): // the sender
				send = h
			case strings.HasSuffix(fn, "pingPong.func1.2"): // the receiver
				recv = h
			}
		}
		for n := range names {
			to = append(to, sched.StartSite(n))
		}
		legal := map[uint64]bool{}
		for _, a := range from {
			for _, b := range to {
				legal[sched.PairKey(a, b)] = true
			}
		}
		for p := range pairs {
			if !legal[p] {
				t.Fatalf("seed %d: pair %x isn't two of the program's own sites (sites %v, tasks %v)", seed, p, sortedNames(sites), sortedNames(names))
			}
		}
		if send == 0 || recv == 0 {
			t.Fatalf("sites not found: %v", sites)
		}
		if pairs[sched.PairKey(send, recv)] > 0 {
			sawHandoff = true
		}
		// Recording pairs doesn't change the run; off, nothing is recorded.
		off, offPairs, _, _ := runPairs(seed, false)
		if off.Fingerprint != r.Fingerprint || !slices.Equal(off.Checkpoints, r.Checkpoints) {
			t.Fatalf("seed %d: recording pairs changed the run", seed)
		}
		if len(offPairs) != 0 {
			t.Fatalf("seed %d: %d pairs recorded with pairs off", seed, len(offPairs))
		}
	}
	if !sawHandoff {
		t.Errorf("no seed recorded the sender handing off to the receiver")
	}
}

func sortedNames(m map[string]bool) []string {
	out := pairKeysOf(m)
	slices.Sort(out)
	return out
}

func pairKeysOf[K comparable, V any](m map[K]V) []K {
	var out []K
	for k := range m {
		out = append(out, k)
	}
	return out
}

// pairsChild, set in a child process, makes TestPairsAcrossProcesses print
// seed 3's pairs instead of checking.
const pairsChild = "DST_PAIRS_CHILD"

func pairList(seed uint64) string {
	_, pairs, _, _ := runPairs(seed, true)
	var out []string
	keys := pairKeysOf(pairs)
	slices.Sort(keys)
	for _, p := range keys {
		out = append(out, fmt.Sprintf("%x@%d", p, pairs[p]))
	}
	return strings.Join(out, ",")
}

// Verifies: [pairs-same-in-every-process]
// Fails if: a new process (other addresses, an empty site cache) records
// other pairs, or other first steps, for the same seed.
func TestPairsAcrossProcesses(t *testing.T) {
	if os.Getenv(pairsChild) != "" {
		fmt.Print("PAIRS:" + pairList(3) + "\n")
		return
	}
	here := pairList(3)
	cmd := exec.Command(os.Args[0], "-test.run=^TestPairsAcrossProcesses$", "-test.count=1")
	cmd.Env = append(os.Environ(), pairsChild+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("child: %v\n%s", err, out)
	}
	_, there, ok := strings.Cut(string(out), "PAIRS:")
	if !ok {
		t.Fatalf("child printed no pairs:\n%s", out)
	}
	there, _, _ = strings.Cut(there, "\n")
	if there != here {
		t.Fatalf("seed 3's pairs differ in a new process:\n here  %.200s\n there %.200s", here, there)
	}
}
