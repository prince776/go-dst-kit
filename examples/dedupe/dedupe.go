// Package dedupe is exactly-once processing over at-least-once delivery: a
// producer retries each message until it is acknowledged, and a consumer
// applies each message once however often it arrives, across its own
// crashes. Ordinary Go (goroutines, a mutex, TCP, files) written against
// a dst.World.
//
// The consumer keeps one write-ahead log of applied message IDs: applying
// a message and remembering that it was applied are the same durable
// record, so no crash can separate them.
//
// Bugs plants known mistakes; the test shows the simulator catches each.
//
// Verified:
//   - [exactly-once] Every message the producer delivers is applied
//     exactly once, under crashes, partitions, resets and power loss.
//   - [eventually-delivered] Once faults stop, every message is
//     acknowledged.
//
// Depends on: dst, examples/wal
//
//dst:simulated
package dedupe

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/examples/wal"
)

// Bugs are mistakes the consumer can be told to make.
type Bugs struct {
	// InMemorySeen keeps the set of applied IDs only in memory: after a
	// crash, a retried message is applied again.
	InMemorySeen bool
	// SeparateSeenFile records "applied" in a second log, after the
	// effect: a crash between the two applies a retry again.
	SeparateSeenFile bool
}

// Consumer applies messages once each.
type Consumer struct {
	w       *dst.World
	bugs    Bugs
	mu      dst.Mutex
	seen    map[string]bool
	effects *wal.Log
	seenLog *wal.Log
}

// EffectsLog is the file holding applied message IDs, in order.
const EffectsLog = "effects"

// NewConsumer recovers a consumer from w's disk.
func NewConsumer(w *dst.World, bugs Bugs) (*Consumer, error) {
	c := &Consumer{w: w, bugs: bugs, seen: map[string]bool{}}
	var err error
	if c.effects, err = wal.Open(w.FS(), EffectsLog, wal.Bugs{}); err != nil {
		return nil, err
	}
	switch {
	case bugs.InMemorySeen:
		// (the bug: nothing is recovered)
	case bugs.SeparateSeenFile:
		if c.seenLog, err = wal.Open(w.FS(), "seen", wal.Bugs{}); err != nil {
			return nil, err
		}
		for _, id := range c.seenLog.Records() {
			c.seen[string(id)] = true
		}
	default:
		for _, id := range c.effects.Records() {
			c.seen[string(id)] = true
		}
	}
	return c, nil
}

// Apply applies message id unless it already was.
func (c *Consumer) Apply(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seen[id] {
		return nil
	}
	if _, err := c.effects.Append([]byte(id)); err != nil {
		return err
	}
	if c.bugs.SeparateSeenFile {
		if _, err := c.seenLog.Append([]byte(id)); err != nil {
			return err
		}
	}
	c.seen[id] = true
	return nil
}

// Serve accepts producers on ln: each line is a message ID, answered with
// "ok <id>" once applied.
func (c *Consumer) Serve(ln net.Listener) error {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		c.w.Go(func() {
			defer conn.Close()
			sc := bufio.NewScanner(conn)
			for sc.Scan() {
				id := sc.Text()
				if err := c.Apply(id); err != nil {
					return
				}
				if _, err := fmt.Fprintf(conn, "ok %s\n", id); err != nil {
					return
				}
			}
		})
	}
}

// Deliver sends message id to the consumer at addr until it is
// acknowledged, retrying with backoff; each attempt has a timeout.
func Deliver(w *dst.World, addr, id string) {
	backoff := 10 * time.Millisecond
	for !try(w, addr, id) {
		w.Sleep(backoff + time.Duration(w.Rand().Int64N(int64(backoff))))
		backoff = min(2*backoff, time.Second)
	}
}

func try(w *dst.World, addr, id string) bool {
	ctx, cancel := dst.WithTimeout(w, context.Background(), 300*time.Millisecond)
	defer cancel()
	conn, err := w.Net().DialContext(ctx, "tcp", addr)
	if err != nil {
		return false
	}
	defer conn.Close()
	dl, _ := ctx.Deadline()
	conn.SetDeadline(dl)
	if _, err := fmt.Fprintf(conn, "%s\n", id); err != nil {
		return false
	}
	line, err := bufio.NewReader(conn).ReadString('\n')
	return err == nil && strings.TrimSpace(line) == "ok "+id
}
