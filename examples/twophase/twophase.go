// Package twophase is two-phase commit: a coordinator moves money between
// accounts held by two participants, atomically, despite crashes of any of
// them and a network that loses, delays and partitions. Ordinary Go
// (goroutines, a mutex, TCP, files) written against a dst.World.
//
// Each participant holds one account. A transfer is a transaction with a
// delta for each participant. The coordinator logs "begin", asks both to
// prepare, logs its decision, and tells them. A participant that votes
// yes has first logged the prepared delta, so it can still commit after a
// crash; while in doubt it holds its lock and asks the coordinator. A
// restarted coordinator aborts every transaction it began but didn't
// decide (presumed abort).
//
// Bugs plants the classic mistakes; the test shows the simulator catches
// each.
//
// Verified:
//   - [atomic] Every transaction commits at both participants or at
//     neither, and the client's answer matches.
//   - [conserves] The total balance never changes and no account goes
//     negative.
//   - [resolves] Once faults stop, no participant stays in doubt.
//
// Depends on: dst, examples/wal
//
//dst:simulated
package twophase

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/examples/wal"
)

// Bugs are mistakes the coordinator and participants can be told to make.
type Bugs struct {
	// DecideAlone makes a participant in doubt for too long abort on its
	// own, though the coordinator may have committed.
	DecideAlone bool
	// SendBeforeLog makes the coordinator tell participants to commit
	// before it logs the decision: if it crashes in between, it recovers
	// and presumes abort.
	SendBeforeLog bool
	// NoPrepareLog makes a participant vote yes without logging the
	// prepared delta: after a crash it can't apply the commit.
	NoPrepareLog bool
}

// LogFile is the log each machine keeps.
const LogFile = "log"

// Initial is each account's starting balance.
const Initial = 100

const timeout = 200 * time.Millisecond

// call sends one request line to addr and returns the reply line.
func call(w *dst.World, addr, req string) (string, error) { return callWithin(w, addr, req, timeout) }

func callWithin(w *dst.World, addr, req string, d time.Duration) (string, error) {
	ctx, cancel := dst.WithTimeout(w, context.Background(), d)
	defer cancel()
	conn, err := w.Net().DialContext(ctx, "tcp", addr)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	if _, err := fmt.Fprintln(conn, req); err != nil {
		return "", err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	return strings.TrimSpace(line), err
}

// serve runs handle for each request line on ln.
func serve(w *dst.World, ln net.Listener, handle func(req string) string) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		w.Go(func() {
			defer c.Close()
			line, err := bufio.NewReader(c).ReadString('\n')
			if err != nil {
				return
			}
			if reply := handle(strings.TrimSpace(line)); reply != "" {
				fmt.Fprintln(c, reply)
			}
		})
	}
}

// ---- participant ----

// Participant holds one account.
type Participant struct {
	w          *dst.World
	coord      string
	bugs       Bugs
	mu         dst.Mutex
	log        *wal.Log
	balance    int64
	prepared   map[int64]int64 // tx -> delta, in doubt
	preparedAt map[int64]time.Time
	outcome    map[int64]string // tx -> "commit" | "abort"
}

// Replay reads a participant's log: its balance, the transactions in
// doubt, and each decided transaction's outcome.
func Replay(records [][]byte) (balance int64, inDoubt map[int64]int64, outcome map[int64]string) {
	balance, inDoubt, outcome = Initial, map[int64]int64{}, map[int64]string{}
	for _, r := range records {
		f := strings.Fields(string(r))
		tx, _ := strconv.ParseInt(f[1], 10, 64)
		switch f[0] {
		case "prepare":
			d, _ := strconv.ParseInt(f[2], 10, 64)
			inDoubt[tx] = d
		case "commit":
			balance += inDoubt[tx]
			delete(inDoubt, tx)
			outcome[tx] = "commit"
		case "abort":
			delete(inDoubt, tx)
			outcome[tx] = "abort"
		}
	}
	return balance, inDoubt, outcome
}

// StartParticipant recovers a participant from w's disk; coord is the
// coordinator's address.
func StartParticipant(w *dst.World, coord string, bugs Bugs) (*Participant, error) {
	l, err := wal.Open(w.FS(), LogFile, wal.Bugs{})
	if err != nil {
		return nil, err
	}
	p := &Participant{w: w, coord: coord, bugs: bugs, log: l, preparedAt: map[int64]time.Time{}}
	p.balance, p.prepared, p.outcome = Replay(l.Records())
	for tx := range p.prepared {
		p.preparedAt[tx] = w.Now()
	}
	return p, nil
}

func (p *Participant) record(s string) error {
	_, err := p.log.Append([]byte(s))
	return err
}

func (p *Participant) handle(req string) string {
	f := strings.Fields(req)
	if len(f) < 2 {
		return ""
	}
	tx, _ := strconv.ParseInt(f[1], 10, 64)
	p.mu.Lock()
	defer p.mu.Unlock()
	switch f[0] {
	case "prepare":
		delta, _ := strconv.ParseInt(f[2], 10, 64)
		if _, ok := p.prepared[tx]; ok {
			return "yes"
		}
		if p.outcome[tx] != "" || len(p.prepared) > 0 || p.balance+delta < 0 {
			return "no" // decided already, locked by another, or no funds
		}
		if !p.bugs.NoPrepareLog {
			if p.record(fmt.Sprintf("prepare %d %d", tx, delta)) != nil {
				return ""
			}
		}
		p.prepared[tx] = delta
		p.preparedAt[tx] = p.w.Now()
		return "yes"
	case "commit", "abort":
		if p.decide(tx, f[0]) != nil {
			return ""
		}
		return "ok"
	}
	return ""
}

// decide applies a decision (with p.mu held).
func (p *Participant) decide(tx int64, d string) error {
	delta, inDoubt := p.prepared[tx]
	if !inDoubt {
		if p.outcome[tx] == "" && d == "commit" {
			// A commit for a transaction we never prepared: only possible
			// if the prepare was lost. Record it so the check sees it.
			if err := p.record(fmt.Sprintf("commit %d", tx)); err != nil {
				return err
			}
			p.outcome[tx] = d
		}
		return nil
	}
	if err := p.record(fmt.Sprintf("%s %d", d, tx)); err != nil {
		return err
	}
	if d == "commit" {
		p.balance += delta
	}
	delete(p.prepared, tx)
	delete(p.preparedAt, tx)
	p.outcome[tx] = d
	return nil
}

func (p *Participant) inDoubt(tx int64) bool {
	_, ok := p.prepared[tx]
	return ok
}

// resolve asks the coordinator about transactions in doubt, forever. Its
// request timeout doubles while requests fail (up to 5s) and resets after
// one succeeds: a fixed timeout shorter than a slow link's round trip
// would leave a transaction in doubt forever.
func (p *Participant) resolve() {
	wait := timeout
	for {
		p.w.Sleep(50 * time.Millisecond)
		p.mu.Lock()
		var txs []int64
		for tx := range p.prepared {
			txs = append(txs, tx)
		}
		slices.Sort(txs) // map order isn't deterministic
		p.mu.Unlock()
		for _, tx := range txs {
			d, err := callWithin(p.w, p.coord, fmt.Sprintf("status %d", tx), wait)
			if err != nil {
				wait = min(2*wait, 5*time.Second)
			} else {
				wait = timeout
			}
			p.mu.Lock()
			switch {
			case err == nil && (d == "commit" || d == "abort"):
				p.decide(tx, d)
			case p.bugs.DecideAlone && p.inDoubt(tx) && p.w.Since(p.preparedAt[tx]) > 300*time.Millisecond:
				p.decide(tx, "abort")
			}
			p.mu.Unlock()
		}
	}
}

// Serve answers the coordinator on ln and resolves doubts.
func (p *Participant) Serve(ln net.Listener) error {
	p.w.Go(p.resolve)
	return serve(p.w, ln, p.handle)
}

// ---- coordinator ----

// Coordinator runs transfers between participants.
type Coordinator struct {
	w        *dst.World
	parts    []string
	bugs     Bugs
	mu       dst.Mutex
	log      *wal.Log
	next     int64
	inflight map[int64]bool
	decided  map[int64]string
}

// StartCoordinator recovers a coordinator from w's disk, aborting the
// transactions it began and didn't decide.
func StartCoordinator(w *dst.World, participants []string, bugs Bugs) (*Coordinator, error) {
	l, err := wal.Open(w.FS(), LogFile, wal.Bugs{})
	if err != nil {
		return nil, err
	}
	c := &Coordinator{w: w, parts: participants, bugs: bugs, log: l, inflight: map[int64]bool{}, decided: map[int64]string{}}
	var begun []int64
	for _, r := range l.Records() {
		f := strings.Fields(string(r))
		tx, _ := strconv.ParseInt(f[1], 10, 64)
		if f[0] == "begin" {
			begun = append(begun, tx)
			c.next = max(c.next, tx)
		} else {
			c.decided[tx] = f[0]
		}
	}
	for _, tx := range begun {
		if c.decided[tx] == "" {
			if _, err := l.Append(fmt.Appendf(nil, "abort %d", tx)); err != nil {
				return nil, err
			}
			c.decided[tx] = "abort"
		}
	}
	return c, nil
}

func (c *Coordinator) logDecision(tx int64, d string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, err := c.log.Append(fmt.Appendf(nil, "%s %d", d, tx)); err != nil {
		return err
	}
	c.decided[tx] = d
	delete(c.inflight, tx)
	return nil
}

// transfer moves amount from participant from to participant to.
func (c *Coordinator) transfer(from, to int, amount int64) (int64, string, error) {
	c.mu.Lock()
	c.next++
	tx := c.next
	c.inflight[tx] = true
	_, err := c.log.Append(fmt.Appendf(nil, "begin %d", tx))
	if err != nil {
		delete(c.inflight, tx) // never began: its status reads as aborted
	}
	c.mu.Unlock()
	if err != nil {
		return 0, "", err
	}
	// (The first version ranged over a map of deltas here; the runner's
	// determinism check flagged it: map order decided the prepare order.)
	d := "commit"
	for _, pd := range []struct {
		part  int
		delta int64
	}{{from, -amount}, {to, amount}} {
		if r, err := call(c.w, c.parts[pd.part], fmt.Sprintf("prepare %d %d", tx, pd.delta)); err != nil || r != "yes" {
			d = "abort"
		}
	}
	tell := func() {
		for _, p := range c.parts {
			call(c.w, p, fmt.Sprintf("%s %d", d, tx))
		}
	}
	if c.bugs.SendBeforeLog {
		tell()
		c.mustLogDecision(tx, d)
		return tx, d, nil
	}
	c.mustLogDecision(tx, d)
	c.w.Go(tell) // participants also ask; this is only quicker
	return tx, d, nil
}

// mustLogDecision logs the decision, retrying while the disk fails (a full
// disk or an I/O error lasts a while). Giving up would leave the
// transaction in flight for good, and its participants in doubt.
func (c *Coordinator) mustLogDecision(tx int64, d string) {
	for c.logDecision(tx, d) != nil {
		c.w.Sleep(10 * time.Millisecond)
	}
}

func (c *Coordinator) handle(req string) string {
	f := strings.Fields(req)
	switch {
	case len(f) == 4 && f[0] == "transfer":
		from, _ := strconv.Atoi(f[1])
		to, _ := strconv.Atoi(f[2])
		amount, _ := strconv.ParseInt(f[3], 10, 64)
		tx, d, err := c.transfer(from, to, amount)
		if err != nil {
			return ""
		}
		return fmt.Sprintf("%s %d", d, tx)
	case len(f) == 2 && f[0] == "status":
		tx, _ := strconv.ParseInt(f[1], 10, 64)
		c.mu.Lock()
		defer c.mu.Unlock()
		if d := c.decided[tx]; d != "" {
			return d
		}
		if c.inflight[tx] {
			return "pending"
		}
		return "abort" // began in an earlier life: aborted at recovery
	}
	return ""
}

// Serve answers clients and participants on ln.
func (c *Coordinator) Serve(ln net.Listener) error { return serve(c.w, ln, c.handle) }

// Transfer asks the coordinator at addr to move amount from participant
// from to participant to. It returns the transaction and "commit" or
// "abort", or an error if the outcome is unknown.
func Transfer(w *dst.World, addr string, from, to int, amount int64) (int64, string, error) {
	ctx, cancel := dst.WithTimeout(w, context.Background(), 4*timeout)
	defer cancel()
	conn, err := w.Net().DialContext(ctx, "tcp", addr)
	if err != nil {
		return 0, "", err
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	if _, err := fmt.Fprintf(conn, "transfer %d %d %d\n", from, to, amount); err != nil {
		return 0, "", err
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		return 0, "", err
	}
	var d string
	var tx int64
	if _, err := fmt.Sscanf(line, "%s %d", &d, &tx); err != nil {
		return 0, "", err
	}
	return tx, d, nil
}
