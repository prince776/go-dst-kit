package check

import (
	"sync"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// History records client operations for Linearizable, with times from a
// World's clock. It is safe for concurrent use (the real World's clients
// run in parallel).
//
// Each operation ends in one of three ways:
//   - Return: the client saw the result. The output is known.
//   - Fail: the client learned that it definitely did not happen (for
//     example "not the leader, nothing was proposed"). It is left out of
//     the history.
//   - Abandon (a timeout), or no ending before the run ends: the outcome
//     is unknown. It may still take effect later.
//
// The first ending counts. A reply that arrives after Abandon is ignored,
// because the client has stopped waiting for it.
//
// Verified:
//   - [first-ending-wins] Only the first Return, Fail or Abandon of an
//     operation affects Ops.
//   - [unfinished-is-unknown] An operation that was invoked and never
//     returned or failed appears in Ops with Unknown set.
//   - [failed-is-absent] A failed operation doesn't appear in Ops.
//   - [times-are-the-worlds] Call and return times come from the World's
//     clock, so they are simulated times in a simulation.
type History[I, O any] struct {
	now   func() time.Time
	base  time.Time
	mu    sync.Mutex
	ops   []Op[I, O]
	state []historyState
}

type historyState uint8

const (
	opOpen historyState = iota
	opReturned
	opFailed
	opAbandoned
)

// NewHistory returns an empty history timed by w's clock.
func NewHistory[I, O any](w *dst.World) *History[I, O] {
	return &History[I, O]{now: w.Now, base: w.Now()}
}

// Call is one invoked operation.
type Call[I, O any] struct {
	h  *History[I, O]
	id int
}

// Invoke records that client invoked in, now.
func (h *History[I, O]) Invoke(client int, in I) Call[I, O] {
	at := h.now().Sub(h.base)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ops = append(h.ops, Op[I, O]{Client: client, Input: in, Call: at})
	h.state = append(h.state, opOpen)
	return Call[I, O]{h, len(h.ops) - 1}
}

func (c Call[I, O]) end(s historyState, out O) bool {
	at := c.h.now().Sub(c.h.base)
	c.h.mu.Lock()
	defer c.h.mu.Unlock()
	if c.h.state[c.id] != opOpen {
		return false
	}
	c.h.state[c.id] = s
	if s == opReturned {
		c.h.ops[c.id].Output = out
		c.h.ops[c.id].Return = at
	}
	return true
}

// Return records that the operation completed with out, now. It reports
// whether this was the operation's first ending.
func (c Call[I, O]) Return(out O) bool { return c.end(opReturned, out) }

// Fail records that the operation definitely did not take effect.
func (c Call[I, O]) Fail() bool {
	var zero O
	return c.end(opFailed, zero)
}

// Abandon records that the client stopped waiting: the outcome is unknown.
func (c Call[I, O]) Abandon() bool {
	var zero O
	return c.end(opAbandoned, zero)
}

// Len is the number of operations invoked.
func (h *History[I, O]) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.ops)
}

// Ops returns the history for Linearizable: returned operations,
// abandoned or unfinished ones (Unknown), and not the failed ones.
func (h *History[I, O]) Ops() []Op[I, O] {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []Op[I, O]
	for i, op := range h.ops {
		switch h.state[i] {
		case opReturned:
			out = append(out, op)
		case opOpen, opAbandoned:
			op.Unknown = true
			out = append(out, op)
		}
	}
	return out
}
