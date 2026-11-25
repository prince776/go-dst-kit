package worldtest

import (
	"bytes"
	"errors"
	"io"
	"net"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// NetChecks run against w.Net() of a fresh World.
var NetChecks = []Check{
	{"net/echo", netEcho},
	{"net/stream-in-order", netStream},
	{"net/refused", netRefused},
	{"net/read-deadline", netDeadline},
	{"net/close", netClose},
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
