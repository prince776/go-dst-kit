package worldtest

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// NetChecks run against w.Net() of a fresh World.
var NetChecks = []Check{
	{"net/echo", netEcho},
	{"net/stream-in-order", netStream},
	{"net/refused", netRefused},
	{"net/dial-timeout", netDialTimeout},
	{"net/read-deadline", netDeadline},
	{"net/close", netClose},
	{"udp/echo", udpEcho},
	{"udp/boundaries", udpBoundaries},
	{"udp/connected", udpConnected},
	{"udp/read-deadline", udpDeadline},
	{"udp/close-unblocks", udpClose},
}

func listen(w *dst.World) net.Listener {
	ln, err := w.Net().Listen("tcp", "localhost:0")
	must(err)
	return ln
}

func netEcho(w *dst.World) {
	ln := listen(w)
	defer ln.Close()
	w.Go(func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		io.Copy(c, c)
		c.Close()
	})
	c, err := w.Net().Dial("tcp", ln.Addr().String())
	must(err)
	for _, msg := range []string{"hello", "world", "!"} {
		_, err := c.Write([]byte(msg))
		must(err)
		buf := make([]byte, len(msg))
		_, err = io.ReadFull(c, buf)
		must(err)
		if string(buf) != msg {
			failf("echo %q, want %q", buf, msg)
		}
	}
	c.Close()
}

func netStream(w *dst.World) {
	ln := listen(w)
	defer ln.Close()
	got := dst.MakeChan[[]byte](w, 1)
	w.Go(func() {
		c, err := ln.Accept()
		if err != nil {
			got.Send(nil)
			return
		}
		b, _ := io.ReadAll(c)
		got.Send(b)
	})
	c, err := w.Net().Dial("tcp", ln.Addr().String())
	must(err)
	var want bytes.Buffer
	for i := range 100 {
		chunk := bytes.Repeat([]byte{byte(i)}, 1+i%7)
		want.Write(chunk)
		_, err := c.Write(chunk)
		must(err)
	}
	must(c.Close())
	if b, _ := got.Recv(); !bytes.Equal(b, want.Bytes()) {
		failf("received %d bytes, want %d in order", len(b), want.Len())
	}
}

func netRefused(w *dst.World) {
	ln := listen(w)
	addr := ln.Addr().String()
	ln.Close()
	if _, err := w.Net().Dial("tcp", addr); err == nil {
		failf("dialing a closed listener succeeded")
	}
	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		failf("Accept after Close: %v", err)
	}
}

func netDeadline(w *dst.World) {
	ln := listen(w)
	defer ln.Close()
	w.Go(func() {
		c, err := ln.Accept()
		if err == nil {
			w.Sleep(200 * time.Millisecond) // say nothing for a while
			c.Close()
		}
	})
	c, err := w.Net().Dial("tcp", ln.Addr().String())
	must(err)
	defer c.Close()
	c.SetReadDeadline(w.Now().Add(20 * time.Millisecond))
	_, err = c.Read(make([]byte, 1))
	var ne net.Error
	if !errors.As(err, &ne) || !ne.Timeout() {
		failf("read past the deadline: %v", err)
	}
}

func netClose(w *dst.World) {
	ln := listen(w)
	defer ln.Close()
	w.Go(func() {
		if c, err := ln.Accept(); err == nil {
			c.Close()
		}
	})
	c, err := w.Net().Dial("tcp", ln.Addr().String())
	must(err)
	if _, err := c.Read(make([]byte, 1)); err != io.EOF {
		failf("read after the peer closed: %v", err)
	}
	must(c.Close())
	if _, err := c.Write([]byte("x")); err == nil {
		failf("write after Close succeeded")
	}
}

func listenUDP(w *dst.World) net.PacketConn {
	pc, err := w.Net().ListenPacket("udp", "localhost:0")
	must(err)
	return pc
}

// udpServe echoes every datagram back to its sender until pc is closed.
func udpServe(w *dst.World, pc net.PacketConn) {
	w.Go(func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			pc.WriteTo(buf[:n], from)
		}
	})
}

func udpEcho(w *dst.World) {
	srv := listenUDP(w)
	defer srv.Close()
	udpServe(w, srv)
	cl := listenUDP(w)
	defer cl.Close()
	_, err := cl.WriteTo([]byte("ping"), srv.LocalAddr())
	must(err)
	buf := make([]byte, 64)
	cl.SetReadDeadline(w.Now().Add(5 * time.Second))
	n, from, err := cl.ReadFrom(buf)
	must(err)
	if string(buf[:n]) != "ping" || from.String() != srv.LocalAddr().String() {
		failf("got %q from %v, want \"ping\" from %v", buf[:n], from, srv.LocalAddr())
	}
}

// Datagrams arrive whole: each read returns exactly one datagram.
func udpBoundaries(w *dst.World) {
	srv := listenUDP(w)
	defer srv.Close()
	cl := listenUDP(w)
	defer cl.Close()
	sizes := map[int]bool{1: true, 100: true, 1200: true}
	for n := range sizes {
		_, err := cl.WriteTo(bytes.Repeat([]byte{byte(n)}, n), srv.LocalAddr())
		must(err)
	}
	srv.SetReadDeadline(w.Now().Add(5 * time.Second))
	buf := make([]byte, 2048)
	for range len(sizes) {
		n, _, err := srv.ReadFrom(buf)
		must(err)
		if !sizes[n] || !bytes.Equal(buf[:n], bytes.Repeat([]byte{byte(n)}, n)) {
			failf("read a %d-byte datagram, not one of those sent whole", n)
		}
		delete(sizes, n)
	}
}

func udpConnected(w *dst.World) {
	srv := listenUDP(w)
	defer srv.Close()
	udpServe(w, srv)
	c, err := w.Net().Dial("udp", srv.LocalAddr().String())
	must(err)
	defer c.Close()
	if c.RemoteAddr().String() != srv.LocalAddr().String() {
		failf("RemoteAddr %v, want %v", c.RemoteAddr(), srv.LocalAddr())
	}
	_, err = c.Write([]byte("hello"))
	must(err)
	c.SetReadDeadline(w.Now().Add(5 * time.Second))
	buf := make([]byte, 64)
	n, err := c.Read(buf)
	must(err)
	if string(buf[:n]) != "hello" {
		failf("read %q, want \"hello\"", buf[:n])
	}
}

func udpDeadline(w *dst.World) {
	pc := listenUDP(w)
	defer pc.Close()
	start := w.Now()
	pc.SetReadDeadline(start.Add(50 * time.Millisecond))
	_, _, err := pc.ReadFrom(make([]byte, 8))
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		failf("read: %v, want a deadline error", err)
	}
	if w.Since(start) < 50*time.Millisecond {
		failf("deadline fired after %v, want 50ms", w.Since(start))
	}
}

func udpClose(w *dst.World) {
	pc := listenUDP(w)
	done := dst.MakeChan[error](w, 1)
	w.Go(func() {
		_, _, err := pc.ReadFrom(make([]byte, 8))
		done.Send(err)
	})
	w.Sleep(10 * time.Millisecond)
	must(pc.Close())
	if err, _ := done.Recv(); !errors.Is(err, net.ErrClosed) {
		failf("read after close: %v, want net.ErrClosed", err)
	}
}

// netDialTimeout: DialTimeout connects as Dial does, and fails as it does
// on a closed listener.
func netDialTimeout(w *dst.World) {
	ln := listen(w)
	w.Go(func() {
		if c, err := ln.Accept(); err == nil {
			c.Write([]byte("hi"))
			c.Close()
		}
	})
	c, err := dst.DialTimeout(w, "tcp", ln.Addr().String(), time.Minute)
	must(err)
	b, _ := io.ReadAll(c)
	c.Close()
	if string(b) != "hi" {
		failf("read %q over DialTimeout's connection", b)
	}
	addr := ln.Addr().String()
	ln.Close()
	if _, err := dst.DialTimeout(w, "tcp", addr, time.Minute); err == nil {
		failf("DialTimeout to a closed listener succeeded")
	}
}
