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
// Holders' writes are fenced: each grant carries a token that grows, and
// the store rejects a token lower than one it has seen, so a holder that
// paused past its lease (a GC pause, a VM stall) can't overwrite the next
// holder's work.
//
// Verified:
//   - [one-holder] At no moment do two live clients believe they hold the
//     lease, under crashes, partitions and latency spikes.
//   - [writes-are-fenced] The store never accepts a token lower than one it
//     already accepted, under pauses, crashes and partitions.
//   - [lease-is-granted] Once faults stop, some client gets the lease.
//
// Depends on: dst
//
//dst:simulated
package lease

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
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
	// NoFencing makes the store accept every write, whatever its token:
	// a holder that paused (or whose write was delayed) past its lease
	// overwrites the next holder's work.
	NoFencing bool
}

// Server grants leases. Leases are in memory; the generation that makes
// fencing tokens grow across restarts is on disk.
type Server struct {
	w      *dst.World
	bugs   Bugs
	mu     dst.Mutex
	holder string
	until  time.Time
	start  time.Time
	gen    uint64 // this incarnation's generation
	grants uint64
	// OnGrant, if set, is called with each grant (for tests).
	OnGrant func(client string)
}

// NewServer returns a server that has just started. It bumps the
// generation on disk before granting anything, so every token it issues
// is higher than any from before a crash.
func NewServer(w *dst.World, bugs Bugs) *Server {
	s := &Server{w: w, bugs: bugs, start: w.Now()}
	fsys := w.FS()
	b, err := dst.ReadFile(fsys, "gen")
	if err != nil && !errors.Is(err, fs.ErrNotExist) {
		panic(err)
	}
	s.gen, _ = strconv.ParseUint(strings.TrimSpace(string(b)), 10, 64)
	s.gen++
	// Atomic replace: a crash leaves the old generation or the new one.
	f, err := fsys.OpenFile("gen.tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		panic(err)
	}
	fmt.Fprintf(f, "%d\n", s.gen)
	if err := f.Sync(); err != nil {
		panic(err)
	}
	f.Close()
	if err := fsys.Rename("gen.tmp", "gen"); err != nil {
		panic(err)
	}
	if err := dst.SyncDir(fsys, "."); err != nil {
		panic(err)
	}
	return s
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
		s.grants++
		token := s.gen<<32 | s.grants
		if s.OnGrant != nil {
			s.OnGrant(client)
		}
		return fmt.Sprintf("granted %d %d %d", int64(TTL), s.until.UnixNano(), token)
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
	// Token is the fencing token of the client's latest grant.
	Token uint64
	// Store, if set, is the address of a Store the client writes to while
	// it holds the lease.
	Store string
}

// NewClient returns a client named name for the server at addr.
func NewClient(w *dst.World, name, addr string, bugs Bugs) *Client {
	return &Client{w: w, name: name, server: addr, bugs: bugs, MaxDrift: DefaultMaxDrift}
}

// Holds reports whether the client believes it holds the lease now.
func (c *Client) Holds() bool { return c.w.Now().Before(c.End) }

// request sends one command; it reports a grant, when the client's lease
// ends by its own clock, and the grant's fencing token.
func (c *Client) request(cmd string) (granted bool, end time.Time, token uint64) {
	sent := c.w.Now()
	ctx, cancel := dst.WithTimeout(c.w, context.Background(), TTL/4)
	defer cancel()
	conn, err := c.w.Net().DialContext(ctx, "tcp", c.server)
	if err != nil {
		return false, time.Time{}, 0
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	if _, err := fmt.Fprintf(conn, "%s %s\n", cmd, c.name); err != nil {
		return false, time.Time{}, 0
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil || !strings.HasPrefix(line, "granted") {
		return false, time.Time{}, 0
	}
	f := strings.Fields(line) // granted <ttl> <expiry> <token>
	if len(f) != 4 {
		return false, time.Time{}, 0
	}
	token, _ = strconv.ParseUint(f[3], 10, 64)
	from := sent
	if c.bugs.ClockFromReply {
		from = c.w.Now()
	}
	if c.bugs.AbsoluteExpiry {
		n, _ := strconv.ParseInt(f[2], 10, 64)
		return true, time.Unix(0, n), token // the server's clock, read as ours
	}
	if c.bugs.NoDriftMargin {
		return true, from.Add(TTL), token
	}
	// Our clock may run up to MaxDrift slow and the server's up to
	// MaxDrift fast: TTL(1-2e) of ours is at most TTL of theirs. (A large
	// bound's margin also hides smaller timing mistakes, such as counting
	// from the reply: the tests check each at the bound that exposes it.)
	return true, from.Add(time.Duration(float64(TTL) * (1 - 2*c.MaxDrift))), token
}

// Run acquires and renews the lease forever, doing work while it holds it.
func (c *Client) Run() {
	for {
		ok, end, token := c.request("acquire")
		switch {
		case ok:
			c.End, c.Token = end, token
		case c.bugs.NoStepDown && c.Holds():
			c.End = c.End.Add(TTL) // assumes the renewal just got lost
		}
		if c.Holds() {
			// Work (a write, fenced by our token), then renew before the
			// lease ends. The machine may pause anywhere, even here: the
			// write can then reach the store after the lease is gone.
			if c.Store != "" {
				c.write()
			}
			c.w.Sleep(TTL / 3)
		} else {
			c.w.Sleep(time.Duration(c.w.Rand().Int64N(int64(TTL))))
		}
	}
}

// write sends one write, with the client's fencing token, to its store.
func (c *Client) write() {
	ctx, cancel := dst.WithTimeout(c.w, context.Background(), TTL/4)
	defer cancel()
	conn, err := c.w.Net().DialContext(ctx, "tcp", c.Store)
	if err != nil {
		return
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	fmt.Fprintf(conn, "write %d %s\n", c.Token, c.name)
	bufio.NewReader(conn).ReadString('\n')
}

// Store is storage that lease holders write to. It fences: a write whose
// token is lower than one it has accepted comes from a holder whose lease
// is gone, and is rejected.
type Store struct {
	w    *dst.World
	bugs Bugs
	mu   dst.Mutex
	max  uint64
	// Regressed describes the first write accepted with a lower token than
	// an earlier one (empty if none): a stale holder's write got in.
	Regressed string
}

// NewStore returns an empty store.
func NewStore(w *dst.World, bugs Bugs) *Store { return &Store{w: w, bugs: bugs} }

func (st *Store) handle(token uint64, client string) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	if token < st.max {
		if !st.bugs.NoFencing {
			return "rejected"
		}
		if st.Regressed == "" {
			st.Regressed = fmt.Sprintf("accepted token %#x from %s after token %#x", token, client, st.max)
		}
	}
	st.max = max(st.max, token)
	return "ok"
}

// Serve answers "write <token> <client>" requests on ln.
func (st *Store) Serve(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		st.w.Go(func() {
			defer c.Close()
			line, err := bufio.NewReader(c).ReadString('\n')
			if err != nil {
				return
			}
			f := strings.Fields(line)
			if len(f) != 3 || f[0] != "write" {
				return
			}
			token, _ := strconv.ParseUint(f[1], 10, 64)
			fmt.Fprintln(c, st.handle(token, f[2]))
		})
	}
}
