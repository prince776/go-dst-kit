package simworld_test

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simfs"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// logger appends synced records to a log, recording each one as acked
// once its Sync returned.
func logger(acked *[]string, boots *int) func(w *dst.World) {
	return func(w *dst.World) {
		*boots++ // counted on entry: power may fail in the first I/O call
		fsys := w.FS()
		f, err := fsys.OpenFile("log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o644)
		if err != nil {
			panic(err)
		}
		if err := dst.SyncDir(fsys, "."); err != nil {
			panic(err)
		}
		for i := 0; ; i++ {
			rec := fmt.Sprintf("boot%d-rec%d\n", *boots, i)
			if _, err := f.Write([]byte(rec)); err != nil {
				panic(err)
			}
			if err := f.Sync(); err != nil {
				panic(err)
			}
			*acked = append(*acked, rec)
			w.Sleep(time.Millisecond)
		}
	}
}

// Verifies: [crash-restart], [power-loss-is-a-crash], [sync-is-durable]
// Fails if: a restart doesn't rerun the machine on its surviving disk, a
// power loss inside I/O doesn't crash it, or a synced record is lost.
func TestCrashRestart(t *testing.T) {
	sawPowerLoss := false
	for seed := uint64(1); seed <= 30; seed++ {
		var acked []string
		boots := 0
		sim := simworld.New(simworld.Options{Seed: seed})
		db := sim.AddMachine("db", simworld.MachineOptions{Disk: simfs.Options{IOCrashProb: 0.02, TornWriteProb: 0.5}})
		db.Start = logger(&acked, &boots)
		var log []byte
		res := sim.Run(func(w *dst.World) {
			db.Boot()
			for range 20 {
				w.Sleep(5 * time.Millisecond)
				if db.Dead() {
					sawPowerLoss = true // it crashed itself inside an I/O call
				} else {
					db.Crash()
				}
				db.Restart()
			}
			w.Sleep(5 * time.Millisecond)
			db.Crash()
			log, _ = dst.ReadFile(db.World.FS(), "log")
		})
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if boots != 21 {
			t.Fatalf("seed %d: %d boots, want 21", seed, boots)
		}
		for _, rec := range acked {
			if !bytes.Contains(log, []byte(rec)) {
				t.Fatalf("seed %d: acked record %q lost", seed, strings.TrimSpace(rec))
			}
		}
	}
	if !sawPowerLoss {
		t.Fatal("no power loss inside I/O in 30 seeds")
	}
}

// Timers die with the incarnation that set them, so a crashed machine's
// ticker doesn't keep the run alive (a real deadlock is still reported).
//
// Verifies: [dead-timers-stop]
// Fails if: a crashed machine's ticker keeps firing, or the deadlock it
// hid goes unreported.
func TestDeadMachineTimersStop(t *testing.T) {
	sim := simworld.New(simworld.Options{Seed: 1, MaxTime: time.Hour})
	a := sim.Machine("a")
	res := sim.Run(func(w *dst.World) {
		tk := a.World.NewTicker(time.Millisecond)
		_ = tk
		w.Sleep(5 * time.Millisecond)
		a.Crash()
		dst.MakeChan[int](w, 0).Recv() // a real deadlock
	})
	var de *sched.DeadlockError
	if !errors.As(res.Err, &de) {
		t.Fatalf("got %v, want a deadlock report", res.Err)
	}
}

// A run that never finishes is cut off with an error, not run forever.
//
// Verifies: [runs-end]
// Fails if: a run past MaxTime or MaxSteps keeps going or ends without an
// error.
func TestLimits(t *testing.T) {
	spin := func(w *dst.World) {
		for {
			w.Sleep(time.Second)
		}
	}
	var le *sched.LimitError
	if r := simworld.Run(simworld.Options{Seed: 1, MaxTime: time.Minute}, spin); !errors.As(r.Err, &le) {
		t.Fatalf("time limit: %v", r.Err)
	}
	if r := simworld.Run(simworld.Options{Seed: 1, MaxSteps: 100}, spin); !errors.As(r.Err, &le) {
		t.Fatalf("step limit: %v", r.Err)
	}
}
