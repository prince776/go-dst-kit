package simworld_test

import (
	"encoding/binary"
	"fmt"
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
	var got []uint32
	for _, a := range udpRunAt(t, opts, send, ctl) {
		got = append(got, a.seq)
	}
	return got
}

// arrival is a datagram b received and when, since the run started.
type arrival struct {
	seq uint32
	at  time.Duration
}

func udpRunAt(t *testing.T, opts simnet.Options, send func(w *dst.World, pc net.PacketConn, to net.Addr), ctl func(w *dst.World, sim *simworld.Sim, b *simworld.Machine)) []arrival {
	t.Helper()
	sim := simworld.New(simworld.Options{Seed: 3, Net: opts})
	a := sim.AddMachine("a", simworld.MachineOptions{})
	b := sim.AddMachine("b", simworld.MachineOptions{})
	var got []arrival
	b.Start = func(w *dst.World) {
		start := w.Now()
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
				got = append(got, arrival{binary.BigEndian.Uint32(buf), w.Since(start)})
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

// Verifies: [udp-links]
// Fails if: a lossy link loses none of its datagrams, or not about its
// share, or its loss draws move the network's other draws (what arrives
// isn't the lossless run minus the lost ones, in the same order); a slow
// link doesn't delay datagrams by its added latency; after the network is
// calm a lossy link still loses, or a slow link stops being slow; or (the
// negative) Links with zero shares changes a run.
func TestUDPLinks(t *testing.T) {
	const n = 1000
	base := simnet.Options{UDPLossProb: 0.1, UDPDupProb: 0.05}
	plain := udpRun(t, base, sendN(n, time.Millisecond), nil)

	// Lossy: about 30% more lost, and the rest exactly as before.
	lossy := base
	lossy.Links = simnet.LinkOptions{LossyShare: 1, LossProb: 0.3}
	got := udpRun(t, lossy, sendN(n, time.Millisecond), nil)
	if r := float64(len(got)) / float64(len(plain)); r < 0.6 || r > 0.8 {
		t.Errorf("a link losing 30%% delivered %d of the lossless run's %d datagrams", len(got), len(plain))
	}
	i := 0
	for _, v := range got {
		for i < len(plain) && plain[i] != v {
			i++
		}
		if i == len(plain) {
			t.Fatalf("the lossy run isn't the lossless one minus losses: %v not in order", v)
		}
		i++
	}

	// Zero shares: the run is what it was.
	zero := base
	zero.Links = simnet.LinkOptions{SlowMean: time.Second, LossProb: 0.5}
	if z := udpRun(t, zero, sendN(n, time.Millisecond), nil); fmt.Sprint(z) != fmt.Sprint(plain) {
		t.Errorf("Links with zero shares changed the run")
	}

	// Slow: every datagram at least half the mean late; fast without.
	lateness := func(opts simnet.Options) (lo, hi time.Duration) {
		lo = time.Hour
		for _, a := range udpRunAt(t, opts, sendN(100, 10*time.Millisecond), nil) {
			late := a.at - time.Duration(a.seq)*10*time.Millisecond
			lo, hi = min(lo, late), max(hi, late)
		}
		return lo, hi
	}
	if lo, _ := lateness(simnet.Options{Links: simnet.LinkOptions{SlowShare: 1, SlowMean: 200 * time.Millisecond}}); lo < 100*time.Millisecond {
		t.Errorf("a slow link (+100ms to 300ms) delivered a datagram after %v", lo)
	}
	if _, hi := lateness(simnet.Options{}); hi > 50*time.Millisecond {
		t.Errorf("a normal link took %v", hi)
	}

	// Calm: losses stop, slowness stays.
	calm := func(opts simnet.Options) []arrival {
		return udpRunAt(t, opts, func(w *dst.World, pc net.PacketConn, to net.Addr) {
			w.Sleep(time.Second) // the network is calm by now
			sendN(300, 10*time.Millisecond)(w, pc, to)
		}, func(w *dst.World, sim *simworld.Sim, b *simworld.Machine) {
			w.Sleep(500 * time.Millisecond)
			sim.Net.StopFaults()
		})
	}
	if got := calm(simnet.Options{Links: simnet.LinkOptions{LossyShare: 1, LossProb: 0.3}}); len(got) != 300 {
		t.Errorf("a lossy link delivered %d of 300 datagrams after the network was calm", len(got))
	}
	for _, a := range calm(simnet.Options{Links: simnet.LinkOptions{SlowShare: 1, SlowMean: 200 * time.Millisecond}}) {
		if late := a.at - time.Second - time.Duration(a.seq)*10*time.Millisecond; late < 100*time.Millisecond {
			t.Fatalf("a slow link stopped being slow when the network was calm (%v)", late)
		}
	}
}

// Verifies: [udp-blocked]
// Fails if: a link that blocks UDP delivers a datagram while the network
// is faulty, TCP on it stops flowing, datagrams stay blocked once it is
// calm, or (the negative) a link with no share blocks.
func TestUDPBlocked(t *testing.T) {
	run := func(share float64) (udpFaulty, udpCalm int, tcp string) {
		sim := simworld.New(simworld.Options{Seed: 5, Net: simnet.Options{Links: simnet.LinkOptions{UDPBlockShare: share}}})
		a := sim.AddMachine("a", simworld.MachineOptions{})
		b := sim.AddMachine("b", simworld.MachineOptions{})
		calm := false
		b.Start = func(w *dst.World) {
			pc, _ := w.Net().ListenPacket("udp", "b:7")
			ln, _ := w.Net().Listen("tcp", "b:8")
			w.Go(func() {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				buf := make([]byte, 16)
				n, _ := c.Read(buf)
				tcp = string(buf[:n])
			})
			buf := make([]byte, 8)
			for {
				if _, _, err := pc.ReadFrom(buf); err != nil {
					return
				}
				if calm {
					udpCalm++
				} else {
					udpFaulty++
				}
			}
		}
		a.Start = func(w *dst.World) {
			pc, _ := w.Net().ListenPacket("udp", "a:0")
			c, err := w.Net().Dial("tcp", "b:8")
			if err == nil {
				c.Write([]byte("flows"))
			}
			sendN(100, 10*time.Millisecond)(w, pc, hostAddr("b:7"))
			w.Sleep(time.Second)
			sendN(100, 10*time.Millisecond)(w, pc, hostAddr("b:7"))
		}
		res := sim.Run(func(w *dst.World) {
			b.Boot()
			a.Boot()
			w.Sleep(1500 * time.Millisecond)
			sim.Net.StopFaults()
			calm = true
			w.Sleep(3 * time.Second)
		})
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		return
	}
	if f, c, tcp := run(1); f != 0 || c != 100 || tcp != "flows" {
		t.Errorf("UDP blocked: %d datagrams arrived while faulty (want 0), %d of 100 once calm; TCP got %q", f, c, tcp)
	}
	if f, c, tcp := run(0); f != 100 || c != 100 || tcp != "flows" {
		t.Errorf("no share: %d and %d of 100 datagrams arrived; TCP got %q", f, c, tcp)
	}
}
