package simnet

import (
	"fmt"
	"net"
	"os"
	"syscall"
	"time"
)

// UDP: datagrams between hosts. Each datagram is delivered whole or not at
// all, after its own latency (so datagrams reorder). With network faults
// on, datagrams are lost and duplicated at the rates in Options. A
// partition, a crashed host, or a port nobody listens on drops them (UDP
// never holds traffic); a paused host's datagrams wait in its receive
// buffer, which holds udpBuffer of them and drops the rest.

const (
	udpBuffer = 256   // datagrams a socket's receive buffer holds
	maxUDP    = 65507 // the largest UDP payload over IPv4
)

func isUDP(network string) bool {
	switch network {
	case "udp", "udp4", "udp6":
		return true
	}
	return false
}

// udpAddr is a UDP address on the simulated network.
type udpAddr struct{ addr }

func (a udpAddr) Network() string { return "udp" }

type datagram struct {
	from udpAddr
	data []byte
}

// packetConn is a UDP socket: net.PacketConn.
type packetConn struct {
	n           *Network
	h           *Host
	inc         int
	a           udpAddr
	buf         []datagram
	closed      bool
	rdl, wdl    time.Time
	deadlineGen int
}

// ListenPacket is net.ListenPacket on this machine (UDP only).
func (h *Host) ListenPacket(network, address string) (net.PacketConn, error) {
	h.n.s.Yield()
	opErr := func(err error) error { return &net.OpError{Op: "listen", Net: network, Err: err} }
	if !isUDP(network) {
		return nil, opErr(net.UnknownNetworkError(network))
	}
	a, err := h.resolve(address)
	if err != nil {
		return nil, opErr(err)
	}
	if a.host != h.name {
		return nil, opErr(fmt.Errorf("can't listen on %s from machine %s", a.host, h.name))
	}
	return h.bindUDP(a.port, opErr)
}

func (h *Host) bindUDP(port int, opErr func(error) error) (*packetConn, error) {
	if port == 0 {
		for h.packets[h.nextPort] != nil {
			h.nextPort++
		}
		port = h.nextPort
		h.nextPort++
	}
	if h.packets[port] != nil {
		return nil, opErr(syscall.EADDRINUSE)
	}
	pc := &packetConn{n: h.n, h: h, inc: h.m.Incarnation, a: udpAddr{addr{h.name, port}}}
	h.packets[port] = pc
	return pc, nil
}

func (pc *packetConn) opErr(op string, err error) error {
	return &net.OpError{Op: op, Net: "udp", Source: pc.a, Err: err}
}

// ReadFrom is net.PacketConn.ReadFrom: one datagram, truncated to len(p).
func (pc *packetConn) ReadFrom(p []byte) (int, net.Addr, error) {
	pc.n.s.Yield()
	for {
		switch {
		case pc.closed:
			return 0, nil, pc.opErr("read", net.ErrClosed)
		case len(pc.buf) > 0:
			d := pc.buf[0]
			pc.buf = pc.buf[1:]
			return copy(p, d.data), d.from, nil
		}
		if pc.wait(pc.rdl, "read from udp "+pc.a.String()) {
			return 0, nil, pc.opErr("read", os.ErrDeadlineExceeded)
		}
	}
}

// wait parks until woken or the deadline passes; it reports a timeout.
func (pc *packetConn) wait(deadline time.Time, why string) bool {
	s := pc.n.s
	if deadline.IsZero() {
		s.ParkOn(pc, why)
		return false
	}
	if !pc.h.now().Before(deadline) {
		return true
	}
	gen := pc.deadlineGen
	t := s.At(pc.h.trueAt(deadline), func() {
		if gen == pc.deadlineGen {
			s.WakeAll(pc)
		}
	})
	s.ParkOn(pc, why)
	t.Stop()
	return !pc.h.now().Before(deadline)
}

// WriteTo is net.PacketConn.WriteTo. It never blocks: the datagram is
// handed to the network, which may lose, duplicate or delay it.
func (pc *packetConn) WriteTo(p []byte, to net.Addr) (int, error) {
	n := pc.n
	n.s.Yield()
	switch {
	case pc.closed:
		return 0, pc.opErr("write", net.ErrClosed)
	case !pc.wdl.IsZero() && !pc.h.now().Before(pc.wdl):
		return 0, pc.opErr("write", os.ErrDeadlineExceeded)
	case len(p) > maxUDP:
		return 0, pc.opErr("write", syscall.EMSGSIZE)
	}
	if to == nil {
		return 0, pc.opErr("write", fmt.Errorf("no destination address"))
	}
	dst, err := pc.h.resolve(to.String())
	if err != nil {
		return 0, pc.opErr("write", err)
	}
	if n.hosts[dst.host] == nil {
		return 0, pc.opErr("write", &net.DNSError{Err: "no such host", Name: dst.host, IsNotFound: true})
	}
	copies := 1
	if n.opts.UDPDupProb > 0 && n.rng.Float64() < n.opts.UDPDupProb {
		copies = 2
	}
	for range copies {
		if n.opts.UDPLossProb > 0 && n.rng.Float64() < n.opts.UDPLossProb {
			n.s.Logf("network: lost a datagram %s -> %s", pc.a, dst)
			continue
		}
		data := append([]byte(nil), p...) // the receiver gets its own copy
		from := pc.a
		n.s.At(n.s.Now()+n.latency(), func() { n.deliverUDP(from, dst, data) })
	}
	return len(p), nil
}

func (n *Network) deliverUDP(from udpAddr, to addr, data []byte) {
	if n.cut(from.host, to.host) {
		return // partitions drop datagrams
	}
	h := n.hosts[to.host]
	if h == nil || h.m.Dead() {
		return
	}
	pc := h.packets[to.port]
	if pc == nil || pc.closed || pc.inc != h.m.Incarnation {
		return // nobody listens there
	}
	if len(pc.buf) >= udpBuffer {
		n.s.Logf("network: %s's receive buffer is full; dropped a datagram", pc.a)
		return
	}
	pc.buf = append(pc.buf, datagram{from: from, data: data})
	n.s.WakeAll(pc)
}

func (pc *packetConn) Close() error {
	pc.n.s.Yield()
	pc.close()
	return nil
}

func (pc *packetConn) close() {
	if pc.closed {
		return
	}
	pc.closed = true
	if pc.h.packets[pc.a.port] == pc {
		delete(pc.h.packets, pc.a.port)
	}
	pc.n.s.WakeAll(pc)
}

func (pc *packetConn) LocalAddr() net.Addr { return pc.a }

func (pc *packetConn) SetDeadline(t time.Time) error {
	pc.rdl, pc.wdl = t, t
	pc.deadlineChanged()
	return nil
}

func (pc *packetConn) SetReadDeadline(t time.Time) error {
	pc.rdl = t
	pc.deadlineChanged()
	return nil
}

func (pc *packetConn) SetWriteDeadline(t time.Time) error {
	pc.wdl = t
	pc.deadlineChanged()
	return nil
}

func (pc *packetConn) deadlineChanged() {
	pc.deadlineGen++
	pc.n.s.WakeAll(pc) // waiters recheck against the new deadline
}

// udpConn is a connected UDP socket (Dial("udp", ...)): it sends to its
// peer and, as on real systems, receives only from it.
type udpConn struct {
	*packetConn
	peer udpAddr
}

func (h *Host) dialUDP(network, address string) (net.Conn, error) {
	h.n.s.Yield()
	opErr := func(err error) error { return &net.OpError{Op: "dial", Net: network, Err: err} }
	to, err := h.resolve(address)
	if err != nil {
		return nil, opErr(err)
	}
	if h.n.hosts[to.host] == nil {
		return nil, opErr(&net.DNSError{Err: "no such host", Name: to.host, IsNotFound: true})
	}
	pc, err := h.bindUDP(0, opErr)
	if err != nil {
		return nil, err
	}
	return &udpConn{packetConn: pc, peer: udpAddr{to}}, nil
}

func (c *udpConn) Read(p []byte) (int, error) {
	for {
		k, from, err := c.ReadFrom(p)
		if err != nil {
			return k, err
		}
		if from.String() == c.peer.String() {
			return k, nil
		}
	}
}

func (c *udpConn) Write(p []byte) (int, error) { return c.WriteTo(p, c.peer) }
func (c *udpConn) RemoteAddr() net.Addr        { return c.peer }

var (
	_ net.PacketConn = (*packetConn)(nil)
	_ net.Conn       = (*udpConn)(nil)
)
