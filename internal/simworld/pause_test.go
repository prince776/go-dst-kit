package simworld_test

import (
	"io"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// Verifies: [pause-stops-a-machine]
// Fails if: a paused machine takes steps, wakes from a sleep before it is
// resumed, misses data sent to it while paused, or stops the others.
func TestPause(t *testing.T) {
	sim := simworld.New(simworld.Options{Seed: 1})
	m := sim.AddMachine("m", simworld.MachineOptions{})
	other := sim.AddMachine("other", simworld.MachineOptions{})
	var mTicks, otherTicks int
	var sleptUntil, sawLocal time.Duration
	var got []byte
	m.Start = func(w *dst.World) {
		w.Go(func() {
			for {
				mTicks++
				w.Sleep(time.Millisecond)
			}
		})
		w.Go(func() { // sleeps across the pause: wakes only at resume
			w.Sleep(8 * time.Millisecond)
			w.Sleep(5 * time.Millisecond) // due at 13ms, during the pause
			sleptUntil = sim.S.Now()
			sawLocal = w.Now().Sub(simworld.Epoch)
		})
		ln, _ := w.Net().Listen("tcp", "m:1")
		c, err := ln.Accept()
		if err != nil {
			return
		}
		got, _ = io.ReadAll(c)
	}
	other.Start = func(w *dst.World) {
		w.Go(func() {
			for {
				otherTicks++
				w.Sleep(time.Millisecond)
			}
		})
		w.Sleep(5 * time.Millisecond)
		c, err := w.Net().Dial("tcp", "m:1")
		if err != nil {
			return
		}
		w.Sleep(15 * time.Millisecond) // into the pause
		c.Write([]byte("sent while m was paused"))
		c.Close()
	}
	var atPause, atResume [2]int
	res := sim.Run(func(w *dst.World) {
		m.Boot()
		other.Boot()
		w.Sleep(10 * time.Millisecond)
		m.Pause()
		atPause = [2]int{mTicks, otherTicks}
		w.Sleep(50 * time.Millisecond)
		atResume = [2]int{mTicks, otherTicks}
		m.Resume()
		w.Sleep(20 * time.Millisecond)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if atResume[0] != atPause[0] {
		t.Errorf("m ticked %d times while paused", atResume[0]-atPause[0])
	}
	if atResume[1]-atPause[1] < 40 {
		t.Errorf("other ticked only %d times during m's pause", atResume[1]-atPause[1])
	}
	if mTicks <= atResume[0] {
		t.Error("m didn't run after it was resumed")
	}
	if sleptUntil != 60*time.Millisecond || sawLocal != 60*time.Millisecond {
		t.Errorf("a sleep due during the pause returned at %v (local %v), want at the resume, 60ms", sleptUntil, sawLocal)
	}
	if string(got) != "sent while m was paused" {
		t.Errorf("m read %q", got)
	}
}
