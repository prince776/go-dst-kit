package simworld_test

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simnet"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// udpRun boots a receiver on "b" (port 7) and runs send on "a"; it returns
// the sequence numbers b received, in arrival order. ctl runs on the
// test's main machine alongside, to inject faults.
func udpRun(t *testing.T, opts simnet.Options, send func(w *dst.World, pc net.PacketConn, to net.Addr), ctl func(w *dst.World, sim *simworld.Sim, b *simworld.Machine)) []uint32 {
	t.Helper()
	sim := simworld.New(simworld.Options{Seed: 3, Net: opts})
	a := sim.AddMachine("a", simworld.MachineOptions{})
	b := sim.AddMachine("b", simworld.MachineOptions{})
	var got []uint32
	b.Start = func(w *dst.World) {
		pc, err := w.Net().ListenPacket("udp", "b:7")
		if err != nil {
			t.Errorf("listen: %v", err)
			return
		}
		buf := make([]byte, 64)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n == 4 {
				got = append(got, binary.BigEndian.Uint32(buf))
			}
		}
	}
	a.Start = func(w *dst.World) {
		pc, err := w.Net().ListenPacket("udp", "a:0")
		if err != nil {
			t.Errorf("listen: %v", err)
			return
		}
		send(w, pc, hostAddr("b:7"))
	}
	res := sim.Run(func(w *dst.World) {
		b.Boot()
		a.Boot()
		if ctl != nil {
			ctl(w, sim, b)
		}
		w.Sleep(5 * time.Second)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	return got
}

// hostAddr is a net.Addr naming a simulated host and port.
type hostAddr string

func (a hostAddr) Network() string { return "udp" }
func (a hostAddr) String() string  { return string(a) }

func datagram(i uint32) []byte { return binary.BigEndian.AppendUint32(nil, i) }

// sendN sends datagrams 0..n-1, one per gap.
func sendN(n int, gap time.Duration) func(w *dst.World, pc net.PacketConn, to net.Addr) {
	return func(w *dst.World, pc net.PacketConn, to net.Addr) {
		for i := range n {
			pc.WriteTo(datagram(uint32(i)), to)
			if gap > 0 {
				w.Sleep(gap)
			}
		}
	}
}

func tally(got []uint32) (distinct, dups, reordered int) {
	seen := map[uint32]bool{}
	var hi uint32
	for i, v := range got {
		if seen[v] {
			dups++
		}
		seen[v] = true
		if i > 0 && v < hi {
			reordered++
		}
		hi = max(hi, v)
	}
	return len(seen), dups, reordered
}

// Verifies: [udp-faults]
// Fails if: without faults a datagram is lost or duplicated; with them,
// none is lost, none duplicated, or none reordered.
func TestUDPFaults(t *testing.T) {
	const n = 1000
	clean := udpRun(t, simnet.Options{}, sendN(n, 0), nil)
	if d, dups, _ := tally(clean); d != n || dups != 0 {
		t.Errorf("no faults: %d of %d datagrams arrived, %d duplicates", d, n, dups)
	}
	faulty := udpRun(t, simnet.Options{UDPLossProb: 0.2, UDPDupProb: 0.2}, sendN(n, 0), nil)
	d, dups, reordered := tally(faulty)
	if d >= n || d < n/2 {
		t.Errorf("20%% loss: %d of %d distinct datagrams arrived", d, n)
	}
	if dups == 0 || reordered == 0 {
		t.Errorf("with faults: %d duplicates, %d reordered; want some of each", dups, reordered)
	}
}

// Verifies: [udp-partition-drops]
// Fails if: a datagram sent across a partition arrives, even after it heals.
func TestUDPPartitionDrops(t *testing.T) {
	got := udpRun(t, simnet.Options{}, sendN(300, 10*time.Millisecond), func(w *dst.World, sim *simworld.Sim, b *simworld.Machine) {
		w.Sleep(time.Second)
		sim.Net.Partition([]string{"a"}, []string{"b"})
		w.Sleep(time.Second)
		sim.Net.Heal()
	})
	var before, during, after int
	for _, v := range got {
		switch {
		case v < 95:
			before++
		case v >= 105 && v < 195:
			during++
		case v >= 205:
			after++
		}
	}
	if before == 0 || after == 0 || during != 0 {
		t.Errorf("arrived %d before the partition, %d sent during it (want 0), %d after", before, during, after)
	}
}

// Verifies: [udp-pause-buffers]
// Fails if: datagrams sent to a paused machine are lost while its buffer
// has room, or a full buffer doesn't drop the excess.
func TestUDPPauseBuffers(t *testing.T) {
	got := udpRun(t, simnet.Options{}, func(w *dst.World, pc net.PacketConn, to net.Addr) {
		w.Sleep(time.Second) // b is paused by now
		sendN(400, 0)(w, pc, to)
	}, func(w *dst.World, sim *simworld.Sim, b *simworld.Machine) {
		w.Sleep(500 * time.Millisecond)
		b.Pause()
		w.Sleep(time.Second)
		b.Resume()
	})
	if d, _, _ := tally(got); d != 256 {
		t.Errorf("a paused machine kept %d of 400 datagrams, want its 256-datagram buffer full", d)
	}
}

// Verifies: [udp-crash-drops]
// Fails if: datagrams reach a crashed machine, its old socket survives the
// restart, or the restarted machine can't bind the same port again.
func TestUDPCrash(t *testing.T) {
	sim := simworld.New(simworld.Options{Seed: 1})
	a := sim.AddMachine("a", simworld.MachineOptions{})
	b := sim.AddMachine("b", simworld.MachineOptions{})
	got := map[int][]uint32{} // by incarnation
	b.Start = func(w *dst.World) {
		pc, err := w.Net().ListenPacket("udp", "b:7")
		if err != nil {
			t.Errorf("listen after restart: %v", err)
			return
		}
		inc := b.M.Incarnation
		buf := make([]byte, 8)
		for {
			n, _, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if n == 4 {
				got[inc] = append(got[inc], binary.BigEndian.Uint32(buf))
			}
		}
	}
	a.Start = func(w *dst.World) {
		pc, _ := w.Net().ListenPacket("udp", "a:0")
		sendN(300, 10*time.Millisecond)(w, pc, hostAddr("b:7"))
	}
	res := sim.Run(func(w *dst.World) {
		b.Boot()
		a.Boot()
		w.Sleep(time.Second)
		b.Crash()
		w.Sleep(time.Second)
		b.Restart()
		w.Sleep(2 * time.Second)
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	for inc, vs := range got {
		for _, v := range vs {
			if v >= 105 && v < 195 {
				t.Errorf("incarnation %d received datagram %d, sent while b was down", inc, v)
			}
		}
	}
	if len(got[0]) == 0 || len(got[1]) == 0 {
		t.Errorf("received %d before the crash and %d after the restart; want some of each", len(got[0]), len(got[1]))
	}
}
