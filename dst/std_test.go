package dst_test

import (
	"context"
	crand "crypto/rand"
	"slices"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

// In the real World CryptoReader is crypto/rand.Reader itself, so code
// that checks for the default reader (crypto's FIPS-only mode does) sees
// it; in a simulation it is the machine's replayable stream.
// Fails if: the real World hands out a wrapper, or a simulation the
// real reader.
func TestCryptoReaderIsStd(t *testing.T) {
	if dst.CryptoReader(dst.Current()) != crand.Reader {
		t.Fatal("the real World's CryptoReader isn't crypto/rand.Reader")
	}
	r := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		if dst.CryptoReader(w) == crand.Reader {
			panic("a simulated World's CryptoReader is the real one")
		}
	})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
}

// In a simulation MathRand draws from the machine's stream, so a seed
// replays it, and Seed restarts it from its argument: the same Seed, the
// same numbers.
// Fails if: two runs of a seed draw differently, or Seed doesn't
// restart the stream (a draw after a second Seed(42) differs from one
// after the first).
func TestMathRandReplaysAndSeeds(t *testing.T) {
	draw := func(seed uint64) (before, afterSeed, again []int64) {
		r := simworld.Run(simworld.Options{Seed: seed}, func(w *dst.World) {
			m := w.MathRand()
			for range 4 {
				before = append(before, m.Int63())
			}
			m.Seed(42)
			for range 4 {
				afterSeed = append(afterSeed, m.Int63())
			}
			m.Seed(42)
			for range 4 {
				again = append(again, m.Int63())
			}
		})
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		return
	}
	b1, s1, a1 := draw(1)
	b2, s2, _ := draw(1)
	b3, s3, _ := draw(2)
	if !slices.Equal(b1, b2) || slices.Equal(b1, b3) {
		t.Errorf("seed 1 drew %v then %v; seed 2 %v", b1, b2, b3)
	}
	if !slices.Equal(s1, a1) || !slices.Equal(s1, s3) || slices.Equal(s1, b1) {
		t.Errorf("after Seed(42): %v, again %v, in another run %v (before it %v)", s1, a1, s3, b1)
	}
	_ = s2
}

// onceSlow is made outside any simulation, as a package-level OnceFunc
// is, and used inside one.
var onceSlow = dst.OnceFunc(func() { dst.Current().Sleep(time.Second) })

// A package-level OnceFunc (made outside any simulation) works inside
// one: while its f sleeps, another simulated goroutine calling it waits
// through the World.
// Fails if: the second caller blocks outside the scheduler (sync's
// OnceFunc does: the run then fails as stuck).
func TestOnceFuncMadeOutsideASimulation(t *testing.T) {
	r := simworld.Run(simworld.Options{Seed: 1, Watchdog: 2 * time.Second}, func(w *dst.World) {
		var wg dst.WaitGroup
		for range 2 {
			wg.Go(w, onceSlow)
		}
		wg.Wait()
	})
	if r.Err != nil {
		t.Fatal(r.Err)
	}
}

// AfterDone's stop is context.AfterFunc's: true the first time it stops
// f, false after, on a context that is never done as on one that is,
// and f runs once the context is cancelled if not stopped.
// Fails if stop answers differently from std's (it returned false for a
// never-done context), or f runs when stopped or not when cancelled.
func TestAfterDoneStopIsStd(t *testing.T) {
	w := realworld.New(realworld.Options{})
	for _, ctx := range []context.Context{context.Background(), context.WithValue(context.Background(), struct{}{}, 1)} {
		std := context.AfterFunc(ctx, func() {})
		got := dst.AfterDone(ctx, func() {})
		for i := range 2 {
			if s, g := std(), got(); s != g {
				t.Errorf("stop #%d on a never-done %T: %v, std's %v", i+1, ctx, g, s)
			}
		}
	}
	ctx, cancel := dst.WithCancel(w, context.Background())
	ran := make(chan struct{})
	stop := dst.AfterDone(ctx, func() { close(ran) })
	cancel()
	<-ran
	if stop() {
		t.Error("stop after f ran returned true")
	}
	ctx2, cancel2 := dst.WithCancel(w, context.Background())
	stop2 := dst.AfterDone(ctx2, func() { t.Error("f ran after stop") })
	if !stop2() || stop2() {
		t.Error("stop before cancel: want true, then false")
	}
	cancel2()
}
