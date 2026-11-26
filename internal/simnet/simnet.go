// Package simnet is a simulated network between machines: TCP-like
// connections with seeded latency, partitions and resets, implementing
// net.Listener and net.Conn so code written against the net package runs
// on it.
//
// Its model of TCP:
//
//   - Bytes on a connection arrive in order, exactly once, each write
//     after a seeded latency.
//   - A partition stalls a connection (like TCP retransmitting): nothing
//     crosses it, and after Heal everything held arrives, in order.
//   - A crashed machine is silent (power loss sends nothing): its peers'
//     reads wait, until their deadlines or until the machine restarts and
//     answers their next packet with a reset.
//   - Data sent to a closed connection is answered with a reset, which
//     arrives after anything sent before it and discards what the
//     receiver hasn't read yet (so closing with unread input can destroy
//     a response in flight, as RFC 7230 §6.6 warns).
//   - Dialing an address with no listener is refused; dialing across a
//     partition waits for Heal or the deadline.
//   - ResetProb resets connections at random, as middleboxes and kernels do.
//
// Addresses are "machine:port"; "localhost" is the dialing machine.
//
// Verified:
//   - [behaves-like-net] Without faults, connections behave like the net
//     package's (the net contract suite passes against both).
//   - [stream-is-exact] Bytes arrive in order, exactly once, unless the
//     connection is reset.
//   - [partition-stalls] Nothing crosses a partition; after Heal,
//     everything held arrives in order.
//   - [crash-is-silent] A crashed machine's peers see no EOF; after it
//     restarts, their next write leads to a reset.
//   - [reset-discards-unread] Data sent to a closed connection resets it,
//     and the reset discards what the other end hasn't read.
//
// Depends on: internal/sched
package simnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net"
	"os"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
)

// Options configure the network.
type Options struct {
	// MinLatency plus an exponential tail with mean MeanLatency (capped at
	// 10x) is each write's delay.
	MinLatency, MeanLatency time.Duration
	// ResetProb is the chance that a connection is reset at each write.
	ResetProb float64
}

// Network connects machines.
type Network struct {
	s       *sched.Scheduler
	rng     *rand.Rand
	opts    Options
	epoch   time.Time
	hosts   map[string]*Host
	blocked map[[2]string]bool
	pipes   []*pipe
	dials   []*dial
}

// New returns a network with no machines.
func New(s *sched.Scheduler, epoch time.Time, opts Options) *Network {
	return &Network{s: s, rng: s.Rand("network"), opts: opts, epoch: epoch,
		hosts: map[string]*Host{}, blocked: map[[2]string]bool{}}
}

// Host is one machine's attachment to the network; it implements dst.Net.
type Host struct {
	n         *Network
	name      string
	m         *sched.Machine
	listeners map[int]*listener
	nextPort  int
}

// now is the host machine's local time: deadlines set by code on it are
// in that clock.
func (h *Host) now() time.Time { return h.n.epoch.Add(h.m.LocalNow()) }

// trueAt is the true time at which the host's clock reaches deadline.
func (h *Host) trueAt(deadline time.Time) time.Duration { return h.m.TrueAt(deadline.Sub(h.n.epoch)) }

// Attach connects machine m to the network under its name.
func (n *Network) Attach(m *sched.Machine) *Host {
	h := &Host{n: n, name: m.Name, m: m, listeners: map[int]*listener{}, nextPort: 40000}
	n.hosts[m.Name] = h
	m.OnCrash(func() {
		for _, port := range slices.Sorted(maps.Keys(h.listeners)) {
			h.listeners[port].closeNow()
		}
	})
	return h
}

// Block stops traffic from one machine to another (one way).
func (n *Network) Block(from, to string) {
	n.blocked[[2]string{from, to}] = true
	n.s.Logf("network: block %s -> %s", from, to)
}

// Partition cuts every path between the two groups, both ways.
func (n *Network) Partition(a, b []string) {
	for _, x := range a {
		for _, y := range b {
			n.Block(x, y)
			n.Block(y, x)
		}
	}
}

// Heal removes every block; held traffic flows again, in order.
func (n *Network) Heal() {
	n.blocked = map[[2]string]bool{}
	n.s.Logf("network: heal")
	for _, p := range n.pipes {
		p.pump()
	}
	for _, d := range n.dials {
		d.retry()
	}
}

// Partitioned reports whether any path is blocked.
func (n *Network) Partitioned() bool { return len(n.blocked) > 0 }

func (n *Network) cut(from, to string) bool { return n.blocked[[2]string{from, to}] }

func (n *Network) latency() time.Duration {
	mean := float64(n.opts.MeanLatency)
	tail := time.Duration(min(n.rng.ExpFloat64()*mean, 10*mean))
	return n.opts.MinLatency + tail
}

// ---- addresses ----

type addr struct {
	host string
	port int
}

func (a addr) Network() string { return "tcp" }
func (a addr) String() string  { return net.JoinHostPort(a.host, strconv.Itoa(a.port)) }

func (h *Host) resolve(address string) (addr, error) {
	host, ps, err := net.SplitHostPort(address)
	if err != nil {
		return addr{}, err
	}
	port, err := strconv.Atoi(ps)
	if err != nil || port < 0 || port > 65535 {
		return addr{}, fmt.Errorf("invalid port %q", ps)
	}
	switch host {
	case "", "localhost", "127.0.0.1", "::1", "0.0.0.0", "::":
		host = h.name
	}
	return addr{host, port}, nil
}

func checkNetwork(network string) error {
	switch network {
	case "tcp", "tcp4", "tcp6":
		return nil
	}
	return net.UnknownNetworkError(network)
}

// ---- listening ----

type listener struct {
	h      *Host
	a      addr
	queue  []*endpoint
	closed bool
}

// Listen is net.Listen on this machine.
func (h *Host) Listen(network, address string) (net.Listener, error) {
	h.n.s.Yield()
	opErr := func(err error) error { return &net.OpError{Op: "listen", Net: network, Err: err} }
	if err := checkNetwork(network); err != nil {
		return nil, opErr(err)
	}
	a, err := h.resolve(address)
	if err != nil {
		return nil, opErr(err)
	}
	if a.host != h.name {
		return nil, opErr(fmt.Errorf("can't listen on %s from machine %s", a.host, h.name))
	}
	if a.port == 0 {
		for h.listeners[h.nextPort] != nil {
			h.nextPort++
		}
		a.port = h.nextPort
		h.nextPort++
	}
	if h.listeners[a.port] != nil {
		return nil, opErr(syscall.EADDRINUSE)
	}
	l := &listener{h: h, a: a}
	h.listeners[a.port] = l
	return l, nil
}

func (l *listener) Addr() net.Addr { return l.a }

func (l *listener) Accept() (net.Conn, error) {
	s := l.h.n.s
	s.Yield()
	for {
		if l.closed {
			return nil, &net.OpError{Op: "accept", Net: "tcp", Addr: l.a, Err: net.ErrClosed}
		}
		if len(l.queue) > 0 {
			e := l.queue[0]
			l.queue = l.queue[1:]
			return e, nil
		}
		s.ParkOn(l, "accept on "+l.a.String())
	}
}

func (l *listener) Close() error {
	l.h.n.s.Yield()
	if l.closed {
		return &net.OpError{Op: "close", Net: "tcp", Addr: l.a, Err: net.ErrClosed}
	}
	l.closeNow()
	return nil
}

func (l *listener) closeNow() {
	l.closed = true
	if l.h.listeners[l.a.port] == l {
		delete(l.h.listeners, l.a.port)
	}
	for _, e := range l.queue {
		e.reset() // never accepted
	}
	l.queue = nil
	l.h.n.s.WakeAll(l)
}

// ---- dialing ----

type dial struct {
	h    *Host
	to   addr
	done bool
	conn *endpoint
	err  error
}

// Dial is net.Dial.
func (h *Host) Dial(network, address string) (net.Conn, error) {
	return h.DialContext(context.Background(), network, address)
}

// DialContext is net.Dialer.DialContext; the context's deadline and
// cancellation follow the World.
func (h *Host) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	n, s := h.n, h.n.s
	s.Yield()
	opErr := func(err error) error { return &net.OpError{Op: "dial", Net: network, Err: err} }
	if err := checkNetwork(network); err != nil {
		return nil, opErr(err)
	}
	to, err := h.resolve(address)
	if err != nil {
		return nil, opErr(err)
	}
	if n.hosts[to.host] == nil {
		return nil, opErr(&net.DNSError{Err: "no such host", Name: to.host, IsNotFound: true})
	}
	if err := ctx.Err(); err != nil {
		return nil, opErr(err)
	}
	d := &dial{h: h, to: to}
	n.dials = append(n.dials, d)
	defer func() { n.dials = slices.DeleteFunc(n.dials, func(x *dial) bool { return x == d }) }()
	d.send()
	var timer sched.Timer
	if deadline, ok := ctx.Deadline(); ok {
		timer = s.At(h.trueAt(deadline), func() { d.finish(nil, os.ErrDeadlineExceeded) })
	}
	stop := dst.AfterDone(ctx, func() { d.finish(nil, ctx.Err()) })
	for !d.done {
		s.ParkOn(d, "dial "+to.String())
	}
	stop()
	timer.Stop()
	if d.err != nil {
		return nil, opErr(d.err)
	}
	return d.conn, nil
}

// send delivers the connection request after a latency, unless a partition
// holds it (retried on Heal).
func (d *dial) send() {
	n := d.h.n
	n.s.At(n.s.Now()+n.latency(), func() {
		if d.done || n.cut(d.h.name, d.to.host) {
			return // lost in the partition; Heal retries
		}
		dst := n.hosts[d.to.host]
		reply := func(fn func()) {
			n.s.At(n.s.Now()+n.latency(), func() {
				if d.done || n.cut(d.to.host, d.h.name) {
					return
				}
				fn()
			})
		}
		if dst.m.Dead() {
			return // silence: the dialer waits for its deadline
		}
		l := dst.listeners[d.to.port]
		if l == nil {
			reply(func() { d.finish(nil, syscall.ECONNREFUSED) })
			return
		}
		local := addr{d.h.name, d.h.nextPort}
		d.h.nextPort++
		a, b := newConn(n, d.h, local, dst, d.to)
		l.queue = append(l.queue, b)
		n.s.WakeOne(l)
		reply(func() { d.finish(a, nil) })
	})
}

func (d *dial) retry() {
	if !d.done {
		d.send()
	}
}

func (d *dial) finish(c *endpoint, err error) {
	if d.done {
		return
	}
	d.done, d.conn, d.err = true, c, err
	d.h.n.s.WakeAll(d)
}

// ---- connections ----

type segment struct {
	data []byte
	fin  bool
	rst  bool
	due  time.Duration
}

// pipe is one direction of a connection.
type pipe struct {
	n        *Network
	from, to *endpoint
	segs     []segment
	lastDue  time.Duration
	armed    bool
}

type endpoint struct {
	n           *Network
	h           *Host
	inc         int // the host incarnation that owns this end
	local, rem  addr
	peer        *endpoint
	out         *pipe
	buf         []byte
	eof         bool
	err         error // reset
	closed      bool
	rdl, wdl    time.Time
	deadlineGen int
}

func newConn(n *Network, ha *Host, aa addr, hb *Host, ab addr) (*endpoint, *endpoint) {
	a := &endpoint{n: n, h: ha, inc: ha.m.Incarnation, local: aa, rem: ab}
	b := &endpoint{n: n, h: hb, inc: hb.m.Incarnation, local: ab, rem: aa}
	a.peer, b.peer = b, a
	a.out = &pipe{n: n, from: a, to: b}
	b.out = &pipe{n: n, from: b, to: a}
	n.pipes = append(n.pipes, a.out, b.out)
	return a, b
}

func (p *pipe) push(seg segment) {
	n := p.n
	seg.due = max(n.s.Now()+n.latency(), p.lastDue)
	p.lastDue = seg.due
	p.segs = append(p.segs, seg)
	p.pump()
}

func (p *pipe) pump() {
	if p.armed || len(p.segs) == 0 {
		return
	}
	p.armed = true
	p.n.s.At(p.segs[0].due, func() {
		p.armed = false
		p.deliver()
	})
}

func (p *pipe) deliver() {
	n := p.n
	for len(p.segs) > 0 && p.segs[0].due <= n.s.Now() {
		if n.cut(p.from.h.name, p.to.h.name) {
			return // stalled; Heal pumps again
		}
		to := p.to
		if to.h.m.Dead() {
			p.segs = nil // silence
			return
		}
		seg := p.segs[0]
		p.segs = p.segs[1:]
		if seg.rst {
			to.abort()
			continue
		}
		if to.err != nil {
			continue
		}
		if to.h.m.Incarnation != to.inc || (to.closed && !seg.fin) {
			// The machine restarted, or this end was closed: it answers
			// with a reset, behind whatever it already sent.
			p.segs = nil
			to.out.push(segment{rst: true})
			to.err = syscall.ECONNRESET
			continue
		}
		if seg.fin {
			to.eof = true
		} else {
			to.buf = append(to.buf, seg.data...)
		}
		n.s.WakeAll(to)
	}
	p.pump()
}

// abort breaks the connection at this end: unsent and unread data are
// discarded, and every operation fails with ECONNRESET.
func (e *endpoint) abort() {
	if e.err == nil {
		e.err = syscall.ECONNRESET
	}
	e.out.segs = nil
	e.buf = nil
	e.n.s.WakeAll(e)
}

// reset aborts this end and tells the peer.
func (e *endpoint) reset() {
	if e.err == nil {
		e.abort()
		e.out.push(segment{rst: true})
	}
}

func (e *endpoint) opErr(op string, err error) error {
	return &net.OpError{Op: op, Net: "tcp", Source: e.local, Addr: e.rem, Err: err}
}

// wait parks until woken or the deadline passes; it reports a timeout.
func (e *endpoint) wait(deadline time.Time, why string) bool {
	s := e.n.s
	if !deadline.IsZero() {
		if !e.h.now().Before(deadline) {
			return true
		}
		gen := e.deadlineGen
		t := s.At(e.h.trueAt(deadline), func() {
			if gen == e.deadlineGen {
				s.WakeAll(e)
			}
		})
		s.ParkOn(e, why)
		t.Stop()
		return !e.h.now().Before(deadline)
	}
	s.ParkOn(e, why)
	return false
}

func (e *endpoint) Read(p []byte) (int, error) {
	e.n.s.Yield()
	for {
		switch {
		case e.closed:
			return 0, e.opErr("read", net.ErrClosed)
		case len(e.buf) > 0:
			k := copy(p, e.buf)
			e.buf = e.buf[k:]
			return k, nil
		case e.err != nil:
			return 0, e.opErr("read", e.err)
		case e.eof:
			return 0, io.EOF
		case len(p) == 0:
			return 0, nil
		}
		if e.wait(e.rdl, "read from "+e.rem.String()) {
			return 0, e.opErr("read", os.ErrDeadlineExceeded)
		}
	}
}

func (e *endpoint) Write(p []byte) (int, error) {
	n := e.n
	n.s.Yield()
	switch {
	case e.closed:
		return 0, e.opErr("write", net.ErrClosed)
	case e.err != nil:
		return 0, e.opErr("write", e.err)
	case !e.wdl.IsZero() && !e.h.now().Before(e.wdl):
		return 0, e.opErr("write", os.ErrDeadlineExceeded)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if n.opts.ResetProb > 0 && n.rng.Float64() < n.opts.ResetProb {
		n.s.Logf("network: reset %s -> %s", e.local, e.rem)
		e.reset()
		return 0, e.opErr("write", e.err)
	}
	e.out.push(segment{data: slices.Clone(p)})
	return len(p), nil
}

func (e *endpoint) Close() error {
	e.n.s.Yield()
	if e.closed {
		return e.opErr("close", net.ErrClosed)
	}
	e.closed = true
	if e.err == nil {
		e.out.push(segment{fin: true})
	}
	e.n.s.WakeAll(e)
	return nil
}

func (e *endpoint) LocalAddr() net.Addr  { return e.local }
func (e *endpoint) RemoteAddr() net.Addr { return e.rem }

func (e *endpoint) SetDeadline(t time.Time) error {
	e.SetReadDeadline(t)
	return e.SetWriteDeadline(t)
}

func (e *endpoint) SetReadDeadline(t time.Time) error {
	e.rdl = t
	e.deadlineGen++
	e.n.s.WakeAll(e) // a blocked Read rechecks against the new deadline
	return nil
}

func (e *endpoint) SetWriteDeadline(t time.Time) error {
	e.wdl = t
	return nil
}

var _ net.Conn = (*endpoint)(nil)

// ErrTimeout reports whether err is a deadline timeout.
func ErrTimeout(err error) bool { return errors.Is(err, os.ErrDeadlineExceeded) }

// StopFaults turns off random connection resets (the runner's recovery
// phase).
func (n *Network) StopFaults() { n.opts.ResetProb = 0 }
