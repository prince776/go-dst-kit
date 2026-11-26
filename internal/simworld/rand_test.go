package simworld_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// draws returns what machines a and b each draw from their Rand: numbers,
// CryptoRead and CryptoReader.
func draws(t *testing.T, seed uint64) map[string][]byte {
	t.Helper()
	sim := simworld.New(simworld.Options{Seed: seed})
	got := map[string][]byte{}
	for _, name := range []string{"a", "b"} {
		m := sim.AddMachine(name, simworld.MachineOptions{})
		m.Start = func(w *dst.World) {
			var buf bytes.Buffer
			r := w.Rand()
			for range 4 {
				buf.WriteByte(byte(r.IntN(256)))
			}
			b := make([]byte, 16)
			r.CryptoRead(b)
			buf.Write(b)
			io.CopyN(&buf, dst.CryptoReader(w), 16)
			got[name] = buf.Bytes()
		}
	}
	res := sim.Run(func(w *dst.World) {
		for _, m := range sim.Machines() {
			if m.Name != "main" {
				m.Boot()
			}
		}
		w.Sleep(time.Millisecond) // let them draw
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	return got
}

// Verifies: [rand-replays]
// Fails if: a machine's Rand (numbers, CryptoRead or CryptoReader) draws
// differently for the same seed; or (negative) two machines, or two seeds,
// draw the same.
func TestRandReplays(t *testing.T) {
	one, again, other := draws(t, 7), draws(t, 7), draws(t, 8)
	for _, m := range []string{"a", "b"} {
		if !bytes.Equal(one[m], again[m]) {
			t.Errorf("machine %s drew %x, then %x with the same seed", m, one[m], again[m])
		}
		if bytes.Equal(one[m], other[m]) {
			t.Errorf("machine %s drew %x with two different seeds", m, one[m])
		}
	}
	if bytes.Equal(one["a"], one["b"]) {
		t.Errorf("machines a and b drew the same %x", one["a"])
	}
}
