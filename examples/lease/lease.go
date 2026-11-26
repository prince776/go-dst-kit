// Package lease is a lock service with leases: a server grants one client
// at a time the right to act for a limited time, and clients renew it.
// Ordinary Go (goroutines, TCP, timers) written against a dst.World.
//
// Safety rests on time: a client may act only until its lease ends, and
// the server grants the lease to someone else only after it ended. Both
// sides must measure conservatively:
//
//   - the client counts its lease from when it sent the request (the
//     server granted it later, so it expires later there);
//   - the client shortens its lease by the protocol's drift bound, since
//     its clock may run slower than the server's;
//   - durations, not timestamps, cross the network: clocks disagree;
//   - a client that can't renew stops acting when its lease ends;
//   - a restarted server, which forgot its grants, waits one lease length
//     before granting anything.
//
// Bugs plants the classic mistakes; the test shows the simulator catches
// each.
//
// Verified:
//   - [one-holder] At no moment do two live clients believe they hold the
//     lease, under crashes, partitions and latency spikes.
//   - [lease-is-granted] Once faults stop, some client gets the lease.
//
// Depends on: dst
//
//dst:simulated
package lease

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// TTL is how long a lease lasts.
const TTL = 200 * time.Millisecond

// DefaultMaxDrift is the clock drift a Client tolerates unless told
// otherwise: any machine's clock runs at a rate within 1% of true time.
const DefaultMaxDrift = 0.01

// Bugs are mistakes the server and clients can be told to make.
type Bugs struct {
	// ClockFromReply makes a client count its lease from when the grant
	// arrived, not from when it asked: it overestimates by the network
	// delay.
	ClockFromReply bool
	// NoStepDown makes a client that fails to renew assume it still holds
	// the lease.
	NoStepDown bool
	// ServerForgets makes a restarted server grant at once, though leases
	// it granted before the crash may still be running.
	ServerForgets bool
	// NoDriftMargin makes a client hold its lease for the full TTL by its
	// own clock: if that runs slower than the server's, it outlasts the
	// server's lease.
	NoDriftMargin bool
	// AbsoluteExpiry makes a client trust the expiry time the server
	// sends, read by the client's clock: an offset between the clocks
	// stretches the lease.
	AbsoluteExpiry bool
}

// Server grants leases. Its state is in memory only.
type Server struct {
	w      *dst.World
	bugs   Bugs
	mu     dst.Mutex
	holder string
	until  time.Time
	start  time.Time
	// OnGrant, if set, is called with each grant (for tests).
	OnGrant func(client string)
}

// NewServer returns a server that has just started.
func NewServer(w *dst.World, bugs Bugs) *Server {
	return &Server{w: w, bugs: bugs, start: w.Now()}
}

func (s *Server) handle(cmd, client string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.w.Now()
	switch cmd {
	case "acquire":
		if !s.bugs.ServerForgets && now.Before(s.start.Add(TTL)) {
			return "denied" // a lease from before the restart may be running
		}
		if s.holder != "" && s.holder != client && now.Before(s.until) {
			return "denied"
		}
		s.holder, s.until = client, now.Add(TTL)
		if s.OnGrant != nil {
			s.OnGrant(client)
		}
		return "granted " + strconv.FormatInt(int64(TTL), 10) + " " + strconv.FormatInt(s.until.UnixNano(), 10)
	case "release":
		if s.holder == client {
			s.holder = ""
		}
		return "ok"
	}
	return "error"
}

// Serve answers requests on ln: "acquire <client>" or "release <client>".
func (s *Server) Serve(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		s.w.Go(func() {
			defer c.Close()
			line, err := bufio.NewReader(c).ReadString('\n')
			if err != nil {
				return
			}
			cmd, client, _ := strings.Cut(strings.TrimSpace(line), " ")
			fmt.Fprintf(c, "%s\n", s.handle(cmd, client))
		})
	}
}

// Client holds the lease when it can and does work while it does.
type Client struct {
	w      *dst.World
	name   string
	server string
	bugs   Bugs
	// MaxDrift is the protocol's drift bound: the client shortens its
	// lease by it (default DefaultMaxDrift). Correct for any bound the
	// clocks honour.
	MaxDrift float64
	// End is when the client believes its lease ends (zero: not held).
	End time.Time
}

// NewClient returns a client named name for the server at addr.
func NewClient(w *dst.World, name, addr string, bugs Bugs) *Client {
	return &Client{w: w, name: name, server: addr, bugs: bugs, MaxDrift: DefaultMaxDrift}
}

// Holds reports whether the client believes it holds the lease now.
func (c *Client) Holds() bool { return c.w.Now().Before(c.End) }

// request sends one command; it reports a grant and when the client's
// lease ends by its own clock.
func (c *Client) request(cmd string) (granted bool, end time.Time) {
	sent := c.w.Now()
	ctx, cancel := dst.WithTimeout(c.w, context.Background(), TTL/4)
	defer cancel()
	conn, err := c.w.Net().DialContext(ctx, "tcp", c.server)
	if err != nil {
		return false, time.Time{}
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	if _, err := fmt.Fprintf(conn, "%s %s\n", cmd, c.name); err != nil {
		return false, time.Time{}
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "granted") {
		return false, time.Time{}
	}
	from := sent
	if c.bugs.ClockFromReply {
		from = c.w.Now()
	}
	if c.bugs.AbsoluteExpiry {
		f := strings.Fields(line)
		if len(f) == 3 {
			n, _ := strconv.ParseInt(f[2], 10, 64)
			return true, time.Unix(0, n) // the server's clock, read as ours
		}
	}
	if c.bugs.NoDriftMargin {
		return true, from.Add(TTL)
	}
	// Our clock may run up to MaxDrift slow and the server's up to
	// MaxDrift fast: TTL(1-2e) of ours is at most TTL of theirs. (A large
	// bound's margin also hides smaller timing mistakes, such as counting
	// from the reply: the tests check each at the bound that exposes it.)
	return true, from.Add(time.Duration(float64(TTL) * (1 - 2*c.MaxDrift)))
}

// Run acquires and renews the lease forever, doing work while it holds it.
func (c *Client) Run() {
	for {
		ok, end := c.request("acquire")
		switch {
		case ok:
			c.End = end
		case c.bugs.NoStepDown && c.Holds():
			c.End = c.End.Add(TTL) // assumes the renewal just got lost
		}
		if c.Holds() {
			// Work for a while, then renew before the lease ends.
			c.w.Sleep(TTL / 3)
		} else {
			c.w.Sleep(time.Duration(c.w.Rand().Int64N(int64(TTL))))
		}
	}
}
