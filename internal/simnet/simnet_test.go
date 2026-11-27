package simnet_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simnet"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// pair runs a server on machine "srv" that reads everything into got, and
// returns a client connection from machine "main".
func pair(sim *simworld.Sim, w *dst.World, got *[]byte, gotErr *error) net.Conn {
	srv := sim.Machine("srv")
	ln, err := srv.World.Net().Listen("tcp", "srv:7")
	if err != nil {
		panic(err)
	}
	srv.World.Go(func() {
		c, err := ln.Accept()
		if err != nil {
			*gotErr = err
			return
		}
		*got, *gotErr = io.ReadAll(c)
	})
	c, err := w.Net().Dial("tcp", "srv:7")
	if err != nil {
		panic(err)
	}
	return c
}

// Verifies: [stream-is-exact]
// Fails if: bytes are reordered, duplicated or lost on a connection that
// wasn't reset, or a reset connection delivers anything but a prefix.
func TestStreamIsExact(t *testing.T) {
	resets := 0
	for seed := uint64(1); seed <= 200; seed++ {
		reset := seed%2 == 0
		opts := simworld.Options{Seed: seed, Net: simnet.Options{MinLatency: time.Microsecond, MeanLatency: 5 * time.Millisecond}}
		if reset {
			opts.Net.ResetProb = 0.02
		}
		sim := simworld.New(opts)
		var got, sent []byte
		var gotErr, sendErr error
		res := sim.Run(func(w *dst.World) {
			c := pair(sim, w, &got, &gotErr)
			for i := range 200 {
				chunk := bytes.Repeat([]byte{byte(i)}, 1+w.Rand().IntN(9))
				if _, err := c.Write(chunk); err != nil {
					sendErr = err
					break
				}
				sent = append(sent, chunk...)
			}
			c.Close()
			w.Sleep(time.Second)
		})
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if sendErr != nil || gotErr != nil {
			resets++
			if !bytes.HasPrefix(sent, got) {
				t.Fatalf("seed %d: after a reset, received bytes aren't a prefix of what was sent", seed)
			}
			continue
		}
		if !bytes.Equal(got, sent) {
			t.Fatalf("seed %d: received %d bytes, sent %d (or out of order)", seed, len(got), len(sent))
		}
	}
	if resets == 0 {
		t.Fatal("ResetProb never reset a connection")
	}
}

// Verifies: [partition-stalls]
// Fails if: data crosses a partition, or isn't all delivered in order
// after Heal.
func TestPartitionStalls(t *testing.T) {
	for seed := uint64(1); seed <= 50; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		var got []byte
		var gotErr error
		res := sim.Run(func(w *dst.World) {
			c := pair(sim, w, &got, &gotErr)
			c.Write([]byte("before "))
			w.Sleep(50 * time.Millisecond)
			sim.Net.Partition([]string{"main"}, []string{"srv"})
			c.Write([]byte("during "))
			w.Sleep(50 * time.Millisecond)
			if len(got) > 0 {
				panic("ReadAll returned during the partition")
			}
			sim.Net.Heal()
			c.Write([]byte("after"))
			c.Close()
			w.Sleep(time.Second)
		})
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		if string(got) != "before during after" || gotErr != nil {
			t.Fatalf("seed %d: got %q, %v", seed, got, gotErr)
		}
	}
	// Negative: a partition really stops traffic (reads time out).
	sim := simworld.New(simworld.Options{Seed: 1})
	var timedOut bool
	sim.Run(func(w *dst.World) {
		srv := sim.Machine("srv")
		ln, _ := srv.World.Net().Listen("tcp", "srv:7")
		srv.World.Go(func() {
			c, _ := ln.Accept()
			c.SetReadDeadline(srv.World.Now().Add(100 * time.Millisecond))
			_, err := c.Read(make([]byte, 1))
			timedOut = errors.Is(err, osErrDeadline)
		})
		c, _ := w.Net().Dial("tcp", "srv:7")
		sim.Net.Partition([]string{"main"}, []string{"srv"})
		c.Write([]byte("x"))
		w.Sleep(time.Second)
	})
	if !timedOut {
		t.Fatal("a read across a partition didn't time out")
	}
}

var osErrDeadline = os.ErrDeadlineExceeded

// Verifies: [crash-is-silent]
// Fails if: a crashed machine's peer sees EOF or an error before the
// machine restarts, or doesn't see a reset after it restarts.
func TestCrashIsSilent(t *testing.T) {
	sim := simworld.New(simworld.Options{Seed: 5})
	srv := sim.Machine("srv")
	srv.Start = func(w *dst.World) {
		ln, err := w.Net().Listen("tcp", "srv:7")
		if err != nil {
			panic(err)
		}
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			w.Go(func() { io.Copy(io.Discard, c) })
		}
	}
	var whileDead, afterRestart error
	res := sim.Run(func(w *dst.World) {
		srv.Boot()
		w.Sleep(time.Millisecond)
		c, err := w.Net().Dial("tcp", "srv:7")
		if err != nil {
			panic(err)
		}
		srv.Crash()
		c.Write([]byte("into the void"))
		c.SetReadDeadline(w.Now().Add(100 * time.Millisecond))
		_, whileDead = c.Read(make([]byte, 1)) // must time out, not EOF
		srv.Restart()
		w.Sleep(10 * time.Millisecond)
		c.SetReadDeadline(time.Time{})
		c.Write([]byte("hello again"))
		_, afterRestart = c.Read(make([]byte, 1))
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if !errors.Is(whileDead, osErrDeadline) {
		t.Fatalf("while the peer was dead: %v, want a timeout", whileDead)
	}
	if !errors.Is(afterRestart, syscall.ECONNRESET) {
		t.Fatalf("after the peer restarted: %v, want a reset", afterRestart)
	}
}

// Verifies: [kill-closes-sockets]
// Fails if: a killed process's peer doesn't read what it wrote and then
// EOF promptly, its next write isn't reset, or a dial isn't refused; or
// (the negative) a powered-off machine's peer sees anything but silence:
// EOF, a reset or a refusal instead of its read and dial timing out.
func TestKillClosesSockets(t *testing.T) {
	type result struct {
		got               string
		readErr, afterErr error
		readTook          time.Duration
		dialErr           error
	}
	run := func(kill bool) result {
		sim := simworld.New(simworld.Options{Seed: 3})
		srv := sim.Machine("srv")
		wrote := false
		srv.Start = func(w *dst.World) {
			ln, err := w.Net().Listen("tcp", "srv:7")
			if err != nil {
				panic(err)
			}
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				w.Go(func() {
					c.Write([]byte("greeting"))
					wrote = true
					io.Copy(io.Discard, c)
				})
			}
		}
		var r result
		res := sim.Run(func(w *dst.World) {
			srv.Boot()
			w.Sleep(time.Millisecond)
			c, err := w.Net().Dial("tcp", "srv:7")
			if err != nil {
				panic(err)
			}
			for !wrote {
				w.Sleep(10 * time.Microsecond)
			}
			if kill {
				srv.Kill() // the greeting is still in flight
			} else {
				srv.Crash()
			}
			start := w.Now()
			c.SetReadDeadline(start.Add(time.Second))
			var b []byte
			b, r.readErr = io.ReadAll(c)
			r.got, r.readTook = string(b), w.Now().Sub(start)
			c.SetReadDeadline(w.Now().Add(time.Second))
			c.Write([]byte("anyone there?"))
			w.Sleep(50 * time.Millisecond) // the reply's round trip
			_, r.afterErr = c.Read(make([]byte, 1))
			ctx, cancel := dst.WithTimeout(w, context.Background(), time.Second)
			defer cancel()
			_, r.dialErr = w.Net().DialContext(ctx, "tcp", "srv:7")
		})
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		return r
	}
	k := run(true)
	switch {
	case k.got != "greeting" || k.readErr != nil:
		t.Fatalf("kill: read %q, %v; want the greeting, then EOF", k.got, k.readErr)
	case k.readTook > 100*time.Millisecond:
		t.Fatalf("kill: EOF took %v, want it at once (a network latency)", k.readTook)
	case !errors.Is(k.afterErr, syscall.ECONNRESET):
		t.Fatalf("kill: a write then read after EOF: %v, want a reset", k.afterErr)
	case !errors.Is(k.dialErr, syscall.ECONNREFUSED):
		t.Fatalf("kill: dial: %v, want connection refused", k.dialErr)
	}
	c := run(false)
	switch {
	case !errors.Is(c.readErr, osErrDeadline):
		t.Fatalf("power loss: read %q, %v; want silence until the deadline", c.got, c.readErr)
	case !errors.Is(c.afterErr, osErrDeadline):
		t.Fatalf("power loss: a later read: %v; want silence", c.afterErr)
	case c.dialErr == nil || errors.Is(c.dialErr, syscall.ECONNREFUSED):
		t.Fatalf("power loss: dial: %v; want it to time out", c.dialErr)
	}
}

// Verifies: [reset-discards-unread]
// Fails if: closing with unread input never destroys a response in flight
// (a kinder network than TCP), or a response arrives corrupted.
func TestCloseWithUnreadInputResets(t *testing.T) {
	lost, delivered := 0, 0
	for seed := uint64(1); seed <= 100; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		srv := sim.Machine("srv")
		var got []byte
		var err error
		res := sim.Run(func(w *dst.World) {
			ln, _ := srv.World.Net().Listen("tcp", "srv:7")
			srv.World.Go(func() {
				c, _ := ln.Accept()
				c.Write([]byte("response"))
				c.Close() // without reading the client's request
			})
			c, _ := w.Net().Dial("tcp", "srv:7")
			c.Write([]byte("request the server never reads"))
			w.Sleep(50 * time.Millisecond)
			got, err = io.ReadAll(c)
		})
		if res.Err != nil {
			t.Fatalf("seed %d: %v", seed, res.Err)
		}
		switch {
		case err == nil && string(got) == "response":
			delivered++
		case errors.Is(err, syscall.ECONNRESET) && (len(got) == 0 || string(got) == "response"):
			lost++
		default:
			t.Fatalf("seed %d: got %q, %v", seed, got, err)
		}
	}
	if lost == 0 {
		t.Fatal("closing with unread input never reset the connection")
	}
	t.Logf("response delivered %d times, destroyed by the reset %d times", delivered, lost)
}

// sendAll sends n chunks main→srv and returns what srv received, whether
// both ends were clean, and the time the transfer took.
func sendAll(t *testing.T, opts simworld.Options, n int) (sent, got []byte, ok bool, took time.Duration) {
	t.Helper()
	sim := simworld.New(opts)
	var gotErr, sendErr error
	res := sim.Run(func(w *dst.World) {
		start := w.Now()
		c := pair(sim, w, &got, &gotErr)
		for i := range n {
			chunk := bytes.Repeat([]byte{byte(i)}, 1+w.Rand().IntN(9))
			if _, err := c.Write(chunk); err != nil {
				sendErr = err
				break
			}
			sent = append(sent, chunk...)
		}
		c.Close()
		for got == nil && gotErr == nil {
			w.Sleep(time.Millisecond)
		}
		took = w.Since(start)
	})
	if res.Err != nil {
		t.Fatalf("seed %d: %v", opts.Seed, res.Err)
	}
	return sent, got, sendErr == nil && gotErr == nil, took
}

// Verifies: [loss-delays]
// Fails if: a lossy link loses, duplicates or reorders bytes, or (negative)
// doesn't delay the stream, or loses nothing at all.
func TestLossDelays(t *testing.T) {
	losses := 0
	var slower int
	for seed := uint64(1); seed <= 100; seed++ {
		base := simworld.Options{Seed: seed, Net: simnet.Options{MinLatency: time.Millisecond, MeanLatency: time.Millisecond}}
		lossy := base
		lossy.Net.Links = simnet.LinkOptions{LossyShare: 1, LossProb: 0.2}
		lossy.Net.Note = func(e string) {
			if e == "link lost a segment" {
				losses++
			}
		}
		sent, got, ok, took := sendAll(t, lossy, 100)
		if !ok || !bytes.Equal(got, sent) {
			t.Fatalf("seed %d: received %d bytes of %d (or out of order), clean %v", seed, len(got), len(sent), ok)
		}
		if _, _, _, plain := sendAll(t, base, 100); took > plain {
			slower++
		}
	}
	if losses == 0 || slower < 90 {
		t.Fatalf("%d segments lost; lossy transfers slower in %d of 100 seeds", losses, slower)
	}
}

// Verifies: [links-per-seed]
// Fails if: a link's behaviour depends on the traffic sent before it is
// first used, every link is alike when only some should be slow, or
// (negative) all seeds draw the same links; or Links unset changes a run.
func TestLinksPerSeed(t *testing.T) {
	opts := func(seed uint64) simworld.Options {
		return simworld.Options{Seed: seed, Net: simnet.Options{Links: simnet.LinkOptions{SlowShare: 0.5, SlowMean: 50 * time.Millisecond, LossyShare: 0.5, LossProb: 0.1}}}
	}
	draw := func(seed uint64, warm bool) []time.Duration {
		sim := simworld.New(opts(seed))
		var out []time.Duration
		sim.Run(func(w *dst.World) {
			if warm { // traffic on another link first
				var got []byte
				var err error
				c := pair(sim, w, &got, &err)
				c.Write([]byte("warm"))
				c.Close()
				w.Sleep(time.Second)
			}
			for _, l := range [][2]string{{"a", "b"}, {"b", "a"}, {"a", "c"}, {"c", "a"}, {"b", "c"}, {"c", "b"}} {
				extra, loss := sim.Net.Link(l[0], l[1])
				out = append(out, extra, time.Duration(loss*1e6))
			}
		})
		return out
	}
	seen := map[string]bool{}
	mixed := false
	for seed := uint64(1); seed <= 20; seed++ {
		a, b := draw(seed, false), draw(seed, true)
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("seed %d: links depend on earlier traffic: %v vs %v", seed, a, b)
		}
		seen[fmt.Sprint(a)] = true
		slow := 0
		for i := 0; i < len(a); i += 2 {
			if a[i] > 0 {
				slow++
			}
		}
		mixed = mixed || (slow > 0 && slow < len(a)/2)
	}
	if len(seen) < 10 || !mixed {
		t.Fatalf("%d distinct link draws in 20 seeds, mixed slow and fast links in a seed: %v", len(seen), mixed)
	}
	// Links unset: the run is what it was.
	for seed := uint64(1); seed <= 20; seed++ {
		plain := simworld.Options{Seed: seed, Net: simnet.Options{MinLatency: time.Millisecond, MeanLatency: time.Millisecond}}
		zeroLinks := plain
		zeroLinks.Net.Links = simnet.LinkOptions{SlowMean: time.Second} // shares 0: off
		_, _, _, a := sendAll(t, plain, 50)
		_, _, _, b := sendAll(t, zeroLinks, 50)
		if a != b {
			t.Fatalf("seed %d: Links with zero shares changed the run (%v vs %v)", seed, a, b)
		}
	}
}

// Verifies: [heal-may-reset]
// Fails if: with HealResetProb 1 a connection that crossed a partition
// survives the heal, or one that didn't cross is reset; or with 0 one is
// reset.
func TestHealMayReset(t *testing.T) {
	run := func(prob float64) (crossErr, sameSideErr error) {
		sim := simworld.New(simworld.Options{Seed: 1, Net: simnet.Options{HealResetProb: prob}})
		sim.Run(func(w *dst.World) {
			srv := sim.Machine("srv")
			ln, _ := srv.World.Net().Listen("tcp", "srv:7")
			srv.World.Go(func() {
				for {
					c, err := ln.Accept()
					if err != nil {
						return
					}
					srv.World.Go(func() { io.Copy(c, c) })
				}
			})
			peer := sim.Machine("peer") // on main's side of the cut
			pln, _ := peer.World.Net().Listen("tcp", "peer:7")
			peer.World.Go(func() {
				c, _ := pln.Accept()
				io.Copy(c, c)
			})
			cross, _ := w.Net().Dial("tcp", "srv:7")
			same, _ := w.Net().Dial("tcp", "peer:7")
			sim.Net.Partition([]string{"main", "peer"}, []string{"srv"})
			w.Sleep(50 * time.Millisecond)
			sim.Net.Heal()
			buf := make([]byte, 2)
			_, crossErr = cross.Write([]byte("hi"))
			if crossErr == nil {
				_, crossErr = io.ReadFull(cross, buf)
			}
			_, sameSideErr = same.Write([]byte("hi"))
			if sameSideErr == nil {
				_, sameSideErr = io.ReadFull(same, buf)
			}
		})
		return
	}
	if c, s := run(1); !errors.Is(c, syscall.ECONNRESET) || s != nil {
		t.Fatalf("HealResetProb 1: crossing connection %v (want a reset), same side %v (want none)", c, s)
	}
	if c, s := run(0); c != nil || s != nil {
		t.Fatalf("HealResetProb 0: crossing %v, same side %v (want both fine)", c, s)
	}
}
