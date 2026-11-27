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
//   - A killed process's host stays up, and its kernel closes the
//     process's sockets: what was written is still delivered, then a FIN
//     (peers read EOF at once); data sent to it afterwards draws a reset;
//     dials to its ports are refused until it listens again.
//   - Data sent to a closed connection is answered with a reset, which
//     arrives after anything sent before it and discards what the
//     receiver hasn't read yet (so closing with unread input can destroy
//     a response in flight, as RFC 7230 §6.6 warns).
//   - Dialing an address with no listener is refused; dialing across a
//     partition waits for Heal or the deadline.
//   - ResetProb resets connections at random, as middleboxes and kernels do.
//   - Links (Options.Links) are each direction between two hosts, with
//     their own behaviour drawn once per seed: a slow link adds latency to
//     every write; a lossy link loses segments, which TCP retransmits, so a
//     loss delays the stream by a retransmission timeout (200ms, doubling
//     while losses repeat, at most 3.2s) and nothing is lost or reordered.
//   - HealResetProb: at Heal, a connection that crossed the partition is
//     reset with this chance (a NAT, firewall or load balancer that forgot
//     the connection answers its next packet with a reset), else it
//     carries on with what was held.
//
// Addresses are "machine:port"; "localhost" is the dialing machine.
//
// Verified:
//   - [behaves-like-net] Without faults, connections behave like the net
//     package's (the net contract suite passes against both).
//   - [stream-is-exact] Bytes arrive in order, exactly once, unless the
//     connection is reset.
//   - [links-per-seed] A link's slowness and loss are drawn once per link
//     and seed, whatever the traffic; with Links unset, the network draws
//     nothing more than before.
//   - [loss-delays] On a lossy link bytes still arrive in order, exactly
//     once; losses only delay them.
//   - [heal-may-reset] With HealResetProb, connections that crossed a
//     partition may be reset at Heal (both ends fail); connections that
//     didn't cross never are; with 0, none is.
//   - [partition-stalls] Nothing crosses a partition; after Heal,
//     everything held arrives in order.
//   - [crash-is-silent] A crashed machine's peers see no EOF; after it
//     restarts, their next write leads to a reset.
//   - [kill-closes-sockets] A killed process's peers read what it wrote,
//     then EOF, at once; their next write is reset; dials to it are
//     refused. Peers of a powered-off machine meanwhile see silence.
//   - [reset-discards-unread] Data sent to a closed connection resets it,
//     and the reset discards what the other end hasn't read.
//
// UDP (udp.go) is datagrams: whole or not at all, each after its own
// latency, lost or duplicated at Options' rates; partitions, crashed
// hosts and unbound ports drop them. Unlike a real kernel, a datagram to
// an unbound port draws no ICMP error, and addresses are host:port names,
// not *net.UDPAddr. Its laws are verified through simworld.
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
	// UDPLossProb and UDPDupProb are the chances that a datagram is lost,
	// or delivered twice.
	UDPLossProb, UDPDupProb float64
	// Links gives links their own behaviour (zero: every link alike).
	Links LinkOptions
	// HealResetProb is the chance that a connection crossing a partition
	// is reset when it heals.
	HealResetProb float64
	// Note, if set, is told of each event a test may want to see happen:
	// "a link is slow", "link lost a segment", "connection reset at heal".
	Note func(event string)
}

// LinkOptions draw each link's behaviour, once per link and seed: with
// chance SlowShare a link is slow, adding a latency around SlowMean (half
// to one and a half times it) to every write and datagram; with chance
// LossyShare it is lossy, losing each segment (retransmitted: TCP waits)
// or datagram (gone: UDP has no retransmission) with chance LossProb; with
// chance UDPBlockShare it drops every datagram while TCP still flows (a
// firewall or security group that blocks UDP).
type LinkOptions struct {
	SlowShare     float64
	SlowMean      time.Duration
	LossyShare    float64
	LossProb      float64
	UDPBlockShare float64
}

func (o LinkOptions) on() bool { return o.SlowShare > 0 || o.LossyShare > 0 || o.UDPBlockShare > 0 }

// link is one direction between two hosts.
type link struct {
	extra      time.Duration // a slow link's added latency (0: not slow)
	loss       float64       // a lossy link's chance of losing a segment or datagram
	udpBlocked bool          // drops every datagram (TCP flows)
}

// link returns the link from one host to another, drawn on first use from
// a configuration stream named after it (so the draw doesn't depend on
// traffic order, and stays put when a run is forked).
func (n *Network) link(from, to string) *link {
	k := [2]string{from, to}
	if l, ok := n.links[k]; ok {
		return l
	}
	r := n.s.ConfigRand("net/link/" + from + ">" + to)
	l := &link{}
	lo := n.opts.Links
	if r.Float64() < lo.SlowShare {
		l.extra = time.Duration(float64(lo.SlowMean) * (0.5 + r.Float64()))
		n.note("a link is slow")
	}
	if r.Float64() < lo.LossyShare {
		l.loss = lo.LossProb
	}
	if r.Float64() < lo.UDPBlockShare { // drawn last: the draws above are as they were
		l.udpBlocked = true
		n.note("a link blocks UDP")
	}
	n.links[k] = l
	return l
}

// Link reports the behaviour of the link from one host to another: its
// added latency and its chance of losing a segment.
func (n *Network) Link(from, to string) (extra time.Duration, loss float64) {
	if !n.opts.Links.on() {
		return 0, 0
	}
	l := n.link(from, to)
	return l.extra, l.loss
}

// UDPBlocked reports whether the link from one host to another drops
// datagrams (while the network isn't calm).
func (n *Network) UDPBlocked(from, to string) bool {
	if !n.opts.Links.on() {
		return false
	}
	return n.link(from, to).udpBlocked
}

func (n *Network) note(event string) {
	if n.opts.Note != nil {
		n.opts.Note(event)
	}
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
	links   map[[2]string]*link
	calm    bool                       // StopFaults: links no longer lose segments or datagrams, nor block UDP
	udpRng  *rand.Rand                 // lossy links' datagram losses (its own stream: other draws stay put)
	cuts    map[[2]string][]sched.Span // when each directed pair was blocked
}

// New returns a network with no machines.
func New(s *sched.Scheduler, epoch time.Time, opts Options) *Network {
	return &Network{s: s, rng: s.Rand("network"), opts: opts, epoch: epoch,
		hosts: map[string]*Host{}, blocked: map[[2]string]bool{}, links: map[[2]string]*link{}, cuts: map[[2]string][]sched.Span{}}
}

// Host is one machine's attachment to the network; it implements dst.Net.
type Host struct {
	n         *Network
	name      string
	m         *sched.Machine
	listeners map[int]*listener
	packets   map[int]*packetConn // UDP sockets by port
	nextPort  int
}

// now is the host machine's local time: deadlines set by code on it are
// in that clock.
func (h *Host) now() time.Time { return h.n.epoch.Add(h.m.LocalNow()) }

// trueAt is the true time at which the host's clock reaches deadline.
func (h *Host) trueAt(deadline time.Time) time.Duration { return h.m.TrueAt(deadline.Sub(h.n.epoch)) }

// Attach connects machine m to the network under its name.
func (n *Network) Attach(m *sched.Machine) *Host {
	h := &Host{n: n, name: m.Name, m: m, listeners: map[int]*listener{}, packets: map[int]*packetConn{}, nextPort: 40000}
	n.hosts[m.Name] = h
	m.OnCrash(func() {
		if m.Killed() {
			// The process died, its host didn't: the kernel closes the
			// process's sockets. What it wrote is still sent, then a FIN;
			// data arriving later is answered with a reset (deliver).
			for _, p := range n.pipes {
				if e := p.from; e.h == h && e.inc == m.Incarnation && !e.closed {
					e.closed = true
					if e.err == nil {
						e.out.push(segment{fin: true})
					}
					n.s.WakeAll(e)
				}
			}
		}
		for _, port := range slices.Sorted(maps.Keys(h.listeners)) {
			h.listeners[port].closeNow()
		}
		for _, port := range slices.Sorted(maps.Keys(h.packets)) {
			h.packets[port].close()
		}
	})
	return h
}

// Block stops traffic from one machine to another (one way).
func (n *Network) Block(from, to string) {
	k := [2]string{from, to}
	if !n.blocked[k] {
		n.cuts[k] = append(n.cuts[k], sched.Span{From: n.s.Now(), Open: true})
	}
	n.blocked[k] = true
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

// Heal removes every block; held traffic flows again, in order, except on
// connections HealResetProb resets.
func (n *Network) Heal() {
	if n.opts.HealResetProb > 0 {
		for _, p := range n.pipes {
			a, b := p.from, p.to
			if a.h.name >= b.h.name {
				continue // each connection once, from its lower-named host's pipe (same-host ones never cross)
			}
			if a.err != nil || b.err != nil || (a.closed && b.closed) {
				continue // already broken, or done
			}
			if !n.blocked[[2]string{a.h.name, b.h.name}] && !n.blocked[[2]string{b.h.name, a.h.name}] {
				continue
			}
			if n.rng.Float64() < n.opts.HealResetProb {
				a.abort()
				b.abort()
				n.note("connection reset at heal")
			}
		}
	}
	for k := range n.blocked { // closing each: order doesn't matter
		if c := n.cuts[k]; len(c) > 0 && c[len(c)-1].Open {
			c[len(c)-1].To, c[len(c)-1].Open = n.s.Now(), false
		}
	}
	n.blocked = map[[2]string]bool{}
	n.s.Logf("network: heal")
	for _, p := range n.pipes {
		p.pump()
	}
	for _, d := range n.dials {
		d.retry()
	}
}

// Cuts is when traffic from one host to another was blocked, each block
// to the heal that ended it (Open while it lasts).
func (n *Network) Cuts(from, to string) []sched.Span {
	return slices.Clone(n.cuts[[2]string{from, to}])
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
	if isUDP(network) {
		return h.dialUDP(network, address) // connectionless: nothing to wait for
	}
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
		if dst.m.Dead() && !dst.m.Killed() {
			return // power loss is silence: the dialer waits for its deadline
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
	rto      time.Duration // the retransmission timeout while losses repeat (lossy links)
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
	d := n.latency()
	if n.opts.Links.on() {
		l := n.link(p.from.h.name, p.to.h.name)
		if l.extra > 0 {
			d += l.extra
		}
		if l.loss > 0 && !n.calm && n.rng.Float64() < l.loss {
			// Lost, and retransmitted after a timeout that doubles while
			// losses repeat: the stream waits, in order.
			p.rto = min(max(200*time.Millisecond, 2*p.rto), 3200*time.Millisecond)
			d += p.rto
			n.note("link lost a segment")
		} else {
			p.rto = 0
		}
	}
	seg.due = max(n.s.Now()+d, p.lastDue)
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
		if to.h.m.Dead() && !to.h.m.Killed() {
			p.segs = nil // power loss is silence
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
func (n *Network) StopFaults() {
	n.opts.ResetProb, n.opts.UDPLossProb, n.opts.UDPDupProb, n.opts.HealResetProb = 0, 0, 0, 0
	n.calm = true // lossy links stop losing, UDP blocks lift; slow links stay slow (a property, not a fault)
}
