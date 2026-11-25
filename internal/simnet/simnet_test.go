package simnet_test

import (
	"bytes"
	"errors"
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
