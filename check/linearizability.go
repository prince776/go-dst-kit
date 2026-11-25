// Package check holds oracles every simulation needs: a linearizability
// checker over client histories, a key-value model for it, and a history
// recorder that understands operations with unknown outcomes (timeouts).
//
// Protocol-specific invariants (one leader per term, identical logs) belong
// in the test that knows the protocol (dsttest's Always and Final).
//
//	hist := check.NewHistory[check.KVInput, check.KVOutput](w)
//	call := hist.Invoke(client, check.KVInput{Op: check.KVPut, Key: "k", Value: v})
//	if err := put(...); err == nil { call.Return(check.KVOutput{}) } else if definitelyNotApplied(err) { call.Fail() } else { call.Abandon() }
//	...
//	h.Final("linearizable", func(*dst.World) error { return check.Linearizable(check.KVModel(), hist.Ops(), check.Options{}).Err() })
//
// Children: Linearizable, Model, History, KVModel
// Depends on: dst (for the clock in NewHistory)
package check

import (
	"cmp"
	"errors"
	"fmt"
	"hash/fnv"
	"slices"
	"strings"
	"time"
)

// Model describes a sequential object that a concurrent history should be
// equivalent to.
//
// S is the object's state. It must be comparable, and the checker compares
// states with ==, so use values, not pointers to mutable data.
type Model[S comparable, I, O any] struct {
	// Init returns the initial state.
	Init func() S
	// Step applies one operation to state and returns whether out is a
	// legal output for it, and the next state. Step must be pure. When
	// known is false the output is unknown (the client timed out): Step
	// must return legal=true and only compute the state change.
	Step func(state S, in I, out O, known bool) (legal bool, next S)
	// Partition, if set, splits the history into independent
	// sub-histories, for example by key. The history is linearizable if
	// and only if every partition is, and checking small partitions is
	// exponentially cheaper.
	Partition func(in I) string
	// Describe, if set, renders an operation in reports.
	Describe func(in I, out O, known bool) string
}

// Op is one completed, abandoned or unfinished client operation.
type Op[I, O any] struct {
	Client int
	Input  I
	Output O
	Call   time.Duration // when the client invoked it
	Return time.Duration // when the client saw the result; ignored if Unknown
	// Unknown marks an operation whose outcome the client never learned
	// (timeout, crash). It may have taken effect at any time after Call,
	// or never.
	Unknown bool
}

// Verdict is the outcome of a linearizability check.
type Verdict int

const (
	// Pass: the history is linearizable. Some order of the operations,
	// consistent with real time, explains every output.
	Pass Verdict = iota
	// Violation: no such order exists.
	Violation
	// Inconclusive: the search ran out of its step budget.
	Inconclusive
)

func (v Verdict) String() string {
	switch v {
	case Pass:
		return "linearizable"
	case Violation:
		return "violation"
	default:
		return "inconclusive"
	}
}

// Result reports a linearizability check.
type Result struct {
	Verdict Verdict
	// Partition is the key of the first partition that failed (or ran out
	// of budget). Empty when the model doesn't partition.
	Partition string
	// Explanation describes the failing partition: its operations, the
	// longest order the search could build, and the operation it could not
	// place. Empty on Pass.
	Explanation string
	// Steps is the total search effort.
	Steps int
}

// ErrInconclusive is what Result.Err returns when the search ran out of
// budget: it could neither prove nor refute linearizability.
var ErrInconclusive = errors.New("check: linearizability inconclusive (search budget exhausted)")

// Err is nil for Pass, an error with the explanation for Violation, and
// an error wrapping ErrInconclusive for Inconclusive.
func (r Result) Err() error {
	switch r.Verdict {
	case Pass:
		return nil
	case Violation:
		return fmt.Errorf("not linearizable (partition %q):\n%s", r.Partition, r.Explanation)
	}
	return fmt.Errorf("%w (partition %q, %d steps)", ErrInconclusive, r.Partition, r.Steps)
}

// Options tune Linearizable.
type Options struct {
	// MaxSteps bounds the search per partition. The bound is in steps, not
	// wall time, so a verdict never depends on how fast the machine is.
	// Zero means 10 million.
	MaxSteps int
}

// Linearizable checks whether history is linearizable with respect to the
// model: whether there is a total order of its operations that respects
// real time and in which each operation's output is what the model
// produces.
//
// a precedes b in real time when a.Return < b.Call. Operations whose
// intervals touch or overlap may be ordered either way. An Unknown
// operation has no return, so it may be placed anywhere after its call,
// including after every other operation, which is how "it never happened"
// is expressed.
//
// The search is the Wing & Gong algorithm with Lowe's memoization of
// (linearized set, state) pairs, the same one Knossos and porcupine use.
// It is exponential in the worst case, so partition the model where the
// object allows it.
//
// Verified:
//   - [matches-brute-force] On every history it decides within budget, its
//     verdict equals that of trying every order of the operations.
//   - [accepts-real-executions] Any history from running the operations one at a
//     time, each inside its interval, is linearizable.
//   - [partitions-independently] With a Partition function, the verdict is
//     Pass exactly when every partition alone is.
//   - [budget-is-deterministic] The same history and options always give
//     the same Result, Steps included.
func Linearizable[S comparable, I, O any](m Model[S, I, O], history []Op[I, O], opts Options) Result {
	if opts.MaxSteps == 0 {
		opts.MaxSteps = 10_000_000
	}
	parts := map[string][]Op[I, O]{}
	if m.Partition == nil {
		parts[""] = history
	} else {
		for _, op := range history {
			k := m.Partition(op.Input)
			parts[k] = append(parts[k], op)
		}
	}
	keys := slices.Sorted(func(yield func(string) bool) {
		for k := range parts {
			if !yield(k) {
				return
			}
		}
	})
	var total int
	var inconclusive *Result
	for _, k := range keys {
		r := checkPartition(m, parts[k], opts.MaxSteps)
		total += r.Steps
		r.Partition = k
		switch r.Verdict {
		case Violation:
			r.Steps = total
			return r
		case Inconclusive:
			if inconclusive == nil {
				rc := r
				inconclusive = &rc
			}
		}
	}
	if inconclusive != nil {
		inconclusive.Steps = total
		return *inconclusive
	}
	return Result{Verdict: Pass, Steps: total}
}

// node is a call or return event in the doubly linked list the search
// lifts operations out of.
type node struct {
	prev, next *node
	op         int
	call       bool
	match      *node // the call's return node
}

func lift(n *node) {
	n.prev.next = n.next
	if n.next != nil {
		n.next.prev = n.prev
	}
	r := n.match
	r.prev.next = r.next
	if r.next != nil {
		r.next.prev = r.prev
	}
}

func unlift(n *node) {
	r := n.match
	r.prev.next = r
	if r.next != nil {
		r.next.prev = r
	}
	n.prev.next = n
	if n.next != nil {
		n.next.prev = n
	}
}

type bitset []uint64

func newBitset(n int) bitset         { return make(bitset, (n+63)/64) }
func (b bitset) set(i int)           { b[i/64] |= 1 << (i % 64) }
func (b bitset) clear(i int)         { b[i/64] &^= 1 << (i % 64) }
func (b bitset) has(i int) bool      { return b[i/64]&(1<<(i%64)) != 0 }
func (b bitset) clone() bitset       { return slices.Clone(b) }
func (b bitset) equal(o bitset) bool { return slices.Equal(b, o) }
func (b bitset) count() (n int) {
	for _, w := range b {
		for ; w != 0; w &= w - 1 {
			n++
		}
	}
	return n
}
func (b bitset) hash() uint64 {
	h := fnv.New64a()
	var buf [8]byte
	for _, w := range b {
		for i := range 8 {
			buf[i] = byte(w >> (8 * i))
		}
		h.Write(buf[:])
	}
	return h.Sum64()
}

type cacheEntry[S comparable] struct {
	lin   bitset
	state S
}

type frame[S comparable] struct {
	n     *node
	state S
}

func checkPartition[S comparable, I, O any](m Model[S, I, O], ops []Op[I, O], maxSteps int) Result {
	n := len(ops)
	if n == 0 {
		return Result{Verdict: Pass}
	}
	type ev struct {
		t    time.Duration
		call bool
		op   int
	}
	evs := make([]ev, 0, 2*n)
	for i, op := range ops {
		evs = append(evs, ev{op.Call, true, i})
		ret := op.Return
		if op.Unknown {
			ret = time.Duration(1<<63 - 1)
		}
		evs = append(evs, ev{ret, false, i})
	}
	// Time order; at equal times calls first (touching intervals overlap);
	// then by operation index, so the search order is deterministic.
	slices.SortStableFunc(evs, func(a, b ev) int {
		if c := cmp.Compare(a.t, b.t); c != 0 {
			return c
		}
		if a.call != b.call {
			if a.call {
				return -1
			}
			return 1
		}
		return cmp.Compare(a.op, b.op)
	})
	head := &node{}
	calls := make([]*node, n)
	rets := make([]*node, n)
	prev := head
	for _, e := range evs {
		nd := &node{prev: prev, op: e.op, call: e.call}
		prev.next = nd
		prev = nd
		if e.call {
			calls[e.op] = nd
		} else {
			rets[e.op] = nd
		}
	}
	for i := range n {
		calls[i].match = rets[i]
	}

	lin := newBitset(n)
	state := m.Init()
	var stack []frame[S]
	cache := map[uint64][]cacheEntry[S]{}
	seen := func(b bitset, s S) bool {
		for _, c := range cache[b.hash()] {
			if c.state == s && c.lin.equal(b) {
				return true
			}
		}
		return false
	}
	var best bitset
	var stuck = -1
	steps := 0
	entry := head.next
	for head.next != nil {
		steps++
		if steps > maxSteps {
			return Result{Verdict: Inconclusive, Steps: steps,
				Explanation: fmt.Sprintf("search stopped after %d steps over %d operations", maxSteps, n)}
		}
		if entry.call {
			op := ops[entry.op]
			legal, next := m.Step(state, op.Input, op.Output, !op.Unknown)
			if legal {
				nl := lin.clone()
				nl.set(entry.op)
				if !seen(nl, next) {
					h := nl.hash()
					cache[h] = append(cache[h], cacheEntry[S]{nl, next})
					stack = append(stack, frame[S]{entry, state})
					state, lin = next, nl
					lift(entry)
					entry = head.next
					continue
				}
			}
			entry = entry.next
			continue
		}
		// A return event: its operation can't be placed any later, so
		// some earlier choice was wrong.
		if best == nil || lin.count() > best.count() {
			best, stuck = lin.clone(), entry.op
		}
		if len(stack) == 0 {
			return Result{Verdict: Violation, Steps: steps, Explanation: explain(m, ops, best, stuck)}
		}
		f := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		state = f.state
		lin.clear(f.n.op)
		unlift(f.n)
		entry = f.n.next
	}
	return Result{Verdict: Pass, Steps: steps}
}

func explain[S comparable, I, O any](m Model[S, I, O], ops []Op[I, O], best bitset, stuck int) string {
	desc := func(op Op[I, O]) string {
		if m.Describe != nil {
			return m.Describe(op.Input, op.Output, !op.Unknown)
		}
		if op.Unknown {
			return fmt.Sprintf("%v -> ?", op.Input)
		}
		return fmt.Sprintf("%v -> %v", op.Input, op.Output)
	}
	order := make([]int, len(ops))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(ops[a].Call, ops[b].Call) })
	var b strings.Builder
	fmt.Fprintf(&b, "%d operations; the longest valid order found places %d of them.\n", len(ops), best.count())
	fmt.Fprintf(&b, "No valid order can place this one before it returned:\n  client %d  [%v, %v]  %s\n",
		ops[stuck].Client, ops[stuck].Call, ops[stuck].Return, desc(ops[stuck]))
	b.WriteString("Operations (by call time; * = placed in that order):\n")
	for _, i := range order {
		op := ops[i]
		mark := " "
		if best.has(i) {
			mark = "*"
		}
		ret := op.Return.String()
		if op.Unknown {
			ret = "?"
		}
		fmt.Fprintf(&b, "  %s client %-3d [%v, %v]  %s\n", mark, op.Client, op.Call, ret, desc(op))
	}
	return b.String()
}
