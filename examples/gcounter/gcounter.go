// Package gcounter is a replicated grow-only counter (a G-Counter CRDT):
// any node accepts increments, nodes gossip their state, and once gossip
// gets through every node reports the same total. Ordinary Go
// (goroutines, TCP, files) written against a dst.World.
//
// Each node counts its own increments in one slot of a vector and
// persists the vector before acknowledging, by the classic atomic file
// replace: write a temporary file, fsync it, rename it over the old one,
// fsync the directory. Merging takes the larger count per slot, so
// gossip can repeat, reorder and duplicate freely.
//
// Bugs plants known mistakes; the test shows the simulator catches each.
//
// Verified:
//   - [converges] Once faults stop, every node reports the same value.
//   - [counts-exactly] The value lies between the acknowledged and the
//     attempted increments. That holds under crashes, partitions, resets
//     and power loss.
//
// Depends on: dst
//
//dst:simulated
package gcounter

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Bugs are mistakes a node can be told to make.
type Bugs struct {
	// MergeSum adds counts on merge instead of taking the maximum, so
	// repeated gossip counts increments again.
	MergeSum bool
	// NoFsyncBeforeRename renames the new state file into place without
	// syncing its data first: after power loss the rename can survive
	// while the data doesn't.
	NoFsyncBeforeRename bool
}

const stateFile = "state"

// Node is one replica.
type Node struct {
	w      *dst.World
	name   string
	peers  []string // addresses of the other nodes
	bugs   Bugs
	mu     dst.Mutex // guards counts; never held across disk I/O
	counts map[string]uint64
	// persistMu serializes increments' writes of the state file. It is
	// separate from mu so that a slow disk delays only increments, not
	// merges, reads and gossip.
	persistMu dst.Mutex
}

// Start recovers a node from w's disk.
func Start(w *dst.World, name string, peers []string, bugs Bugs) (*Node, error) {
	n := &Node{w: w, name: name, peers: peers, bugs: bugs, counts: map[string]uint64{}}
	b, err := dst.ReadFile(w.FS(), stateFile)
	switch {
	case os.IsNotExist(err):
	case err != nil:
		return nil, err
	case len(b) > 0:
		if err := json.Unmarshal(b, &n.counts); err != nil {
			return nil, fmt.Errorf("gcounter: corrupt state: %w", err)
		}
	}
	return n, nil
}

// Value is the counter's value as this node knows it.
func (n *Node) Value() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	var v uint64
	for _, c := range n.counts {
		v += c
	}
	return v
}

// persist writes state b (the counts, as JSON) durably to the state file.
func (n *Node) persist(b []byte) error {
	fsys := n.w.FS()
	f, err := fsys.OpenFile(stateFile+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		return err
	}
	if !n.bugs.NoFsyncBeforeRename {
		if err := f.Sync(); err != nil {
			return err
		}
	}
	if err := fsys.Rename(stateFile+".tmp", stateFile); err != nil {
		return err
	}
	return dst.SyncDir(fsys, ".")
}

// increment counts one increment, durably. Gossip may spread the new
// count before it is on disk; that's allowed, since an increment that
// isn't acknowledged may or may not count.
func (n *Node) increment() error {
	n.persistMu.Lock()
	defer n.persistMu.Unlock()
	n.mu.Lock()
	n.counts[n.name]++
	b, _ := json.Marshal(n.counts) // map keys are sorted: deterministic
	n.mu.Unlock()
	// A failed persist (a full disk, an I/O error) keeps the count: gossip
	// may already have spread it, and a G-counter's entries never go back
	// (a decrement would let the next increment reuse a count peers hold,
	// losing it). The increment wasn't acknowledged, so it may count.
	return n.persist(b)
}

func (n *Node) merge(other map[string]uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, v := range other {
		if n.bugs.MergeSum {
			n.counts[k] += v
		} else {
			n.counts[k] = max(n.counts[k], v)
		}
	}
}

// Serve answers on ln, one request per line: "inc" (answered "ok" once
// durable), "get" (the value) and "merge <json>" (gossip).
func (n *Node) Serve(ln net.Listener) error {
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		n.w.Go(func() {
			defer c.Close()
			line, err := bufio.NewReader(c).ReadString('\n')
			if err != nil {
				return
			}
			cmd, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
			switch cmd {
			case "inc":
				if n.increment() == nil {
					fmt.Fprintln(c, "ok")
				}
			case "get":
				fmt.Fprintln(c, n.Value())
			case "merge":
				var other map[string]uint64
				if json.Unmarshal([]byte(arg), &other) == nil {
					n.merge(other)
					fmt.Fprintln(c, "ok")
				}
			}
		})
	}
}

// Gossip pushes this node's state to a random peer, forever. Each peer has
// its own request timeout, doubled while requests to it fail (up to 5s)
// and reset when one succeeds, so a slow link still carries gossip.
func (n *Node) Gossip() {
	wait := map[string]time.Duration{}
	for {
		n.w.Sleep(time.Duration(20+n.w.Rand().IntN(30)) * time.Millisecond)
		n.mu.Lock()
		b, _ := json.Marshal(n.counts)
		n.mu.Unlock()
		peer := n.peers[n.w.Rand().IntN(len(n.peers))]
		d := max(wait[peer], timeout)
		if _, err := callWithin(n.w, peer, "merge "+string(b), d); err != nil {
			wait[peer] = min(2*d, 2*time.Second) // one slow peer holds up this loop: keep it short
		} else {
			wait[peer] = timeout
		}
	}
}

// timeout is a request's first timeout.
const timeout = 200 * time.Millisecond

// call sends one request and returns the reply line.
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

// Inc asks the node at addr to increment. True means it is counted; false
// means it may or may not be.
func Inc(w *dst.World, addr string) bool {
	r, err := call(w, addr, "inc")
	return err == nil && r == "ok"
}

// Get asks the node at addr for its value, waiting at most d.
func Get(w *dst.World, addr string, d time.Duration) (uint64, error) {
	r, err := callWithin(w, addr, "get", d)
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(r, 10, 64)
}
