package check

import (
	"fmt"
	"strings"
)

// Prefix models a store whose recovered state after a crash must be a
// prefix of the writes made to it: the state after some first s of them,
// with s no less than what was durable. It is a write-ahead log's (or any
// ordered commit log's) contract, and checks it from outside the store:
// record each write as the workload makes it, and after each crash ask
// which prefix the recovered state is.
//
// The adopter supplies the order, by the order of Invoke calls: the
// order the store commits in. Writes that run concurrently must be
// ordered as the store ordered them (by the sequence numbers it gave
// them, say) before they are invoked here.
//
// An op is whatever one write is (a key and value, a batch), and apply
// gives the state after it; apply must not change the state it is
// given. Prefix is not safe for concurrent use: a simulation runs one
// goroutine at a time.
//
// Verified:
//   - [recovery-is-a-prefix] Recover returns the longest prefix at or
//     above the floor whose state equals the recovered one; a state that
//     is no prefix (a write without an earlier one, part of a batch) is a
//     *NotPrefixError naming the closest prefix and how it differs.
//   - [lost-durable-reported] A recovered state that is a prefix only
//     below the floor (it lost writes that were durable) is a
//     *LostDurableError naming the lost writes: a wrong recovery and a
//     lost durable write are both reported, and told apart.
//   - [refused-is-absent] A refused write is gone from every prefix, the
//     later writes' states recomputed without it.
//   - [durable-follows-crashes] After a power loss (Truncate with kill
//     false) what was recovered is durable; after a kill durability
//     stays, at most what was kept.
//   - [window-is-what-a-read-saw] Window is the prefixes a read that ran
//     from since to now may have seen: from the last write acknowledged
//     before it began to the last one invoked.
type Prefix[S, Op any] struct {
	apply func(S, Op) S
	equal func(a, b S) bool
	// Diff, if set, lists how a recovered state differs from a prefix's
	// ("k07: got 12, want 9"): Recover's errors then name the closest
	// prefix and the differences.
	Diff func(got, want S) []string

	ops     []prefixOp[Op]
	states  []S // states[s] is the state after the first s ops
	durable int
	tick    int64
}

type prefixOp[Op any] struct {
	name      string
	op        Op
	invokedAt int64
	ackedAt   int64 // 0: not acknowledged
}

// NewPrefix returns a model of a store that starts in state init. apply
// returns the state after op (without changing the state it is given);
// equal compares states (slices.Equal for a slice, say).
func NewPrefix[S, Op any](init S, apply func(S, Op) S, equal func(a, b S) bool) *Prefix[S, Op] {
	return &Prefix[S, Op]{apply: apply, equal: equal, states: []S{init}}
}

// Now advances the model's logical clock and returns it: stamp a read's
// start with it, for Window.
func (p *Prefix[S, Op]) Now() int64 { p.tick++; return p.tick }

// Invoke records a write about to be made, named for reports; it returns
// its index.
func (p *Prefix[S, Op]) Invoke(name string, op Op) int {
	p.ops = append(p.ops, prefixOp[Op]{name: name, op: op, invokedAt: p.Now()})
	p.states = append(p.states, p.apply(p.states[len(p.states)-1], op))
	return len(p.ops) - 1
}

// Refused records that write i definitely didn't happen: it is dropped,
// and the later writes' states recomputed without it.
func (p *Prefix[S, Op]) Refused(i int) {
	p.ops = append(p.ops[:i], p.ops[i+1:]...)
	p.states = p.states[:i+1]
	for _, o := range p.ops[i:] {
		p.states = append(p.states, p.apply(p.states[len(p.states)-1], o.op))
	}
	if p.durable > i {
		p.durable--
	}
}

// Ack records that write i succeeded; durable says it was durable on
// return (a synced write), which makes every write up to it durable.
func (p *Prefix[S, Op]) Ack(i int, durable bool) {
	p.ops[i].ackedAt = p.Now()
	if durable {
		p.MarkDurable(i + 1)
	}
}

// MarkDurable records that the first n writes are durable (a sync
// returned after them): every crash must keep them.
func (p *Prefix[S, Op]) MarkDurable(n int) { p.durable = max(p.durable, n) }

// Len is the number of writes recorded.
func (p *Prefix[S, Op]) Len() int { return len(p.ops) }

// Durable is the number of first writes that are durable: a power loss
// must keep at least them.
func (p *Prefix[S, Op]) Durable() int { return p.durable }

// Acknowledged is the number of writes up to the last acknowledged one: a
// kill (the process dies, the page cache stays) must keep at least them
// in a store that writes before it acknowledges.
func (p *Prefix[S, Op]) Acknowledged() int {
	n := 0
	for i, o := range p.ops {
		if o.ackedAt != 0 {
			n = i + 1
		}
	}
	return n
}

// State is the state after the first s writes.
func (p *Prefix[S, Op]) State(s int) S { return p.states[s] }

// Name is the name of write i.
func (p *Prefix[S, Op]) Name(i int) string { return p.ops[i].name }

// Recover finds which prefix a recovered state got is: the longest s in
// [floor, Len()] whose state equals it. floor is what the crash had to
// keep: Durable() after a power loss, Acknowledged() after a kill. A
// state that is a prefix only below floor is a *LostDurableError; one
// that is no prefix at all, a *NotPrefixError.
func (p *Prefix[S, Op]) Recover(got S, floor int) (int, error) {
	floor = min(floor, len(p.ops))
	for s := len(p.states) - 1; s >= floor; s-- {
		if p.equal(p.states[s], got) {
			return s, nil
		}
	}
	for s := floor - 1; s >= 0; s-- {
		if p.equal(p.states[s], got) {
			e := &LostDurableError{Recovered: s, Floor: floor}
			for i := s; i < floor; i++ {
				e.Lost = append(e.Lost, p.ops[i].name)
			}
			return 0, e
		}
	}
	e := &NotPrefixError{Floor: floor, Len: len(p.ops), Closest: -1}
	if p.Diff != nil {
		best := 1 << 30
		for s := range p.states {
			if d := p.Diff(got, p.states[s]); len(d) < best || (len(d) == best && s > e.Closest) {
				best, e.Closest, e.Diffs = len(d), s, d
			}
		}
		if e.Closest > 0 {
			e.After = p.ops[e.Closest-1].name
		}
	}
	return 0, e
}

// Truncate keeps the first s writes, as the crash did (s from Recover).
// After a power loss (kill false) what was recovered was on the disk, so
// it is durable now; after a kill it may be only in the page cache, so
// durability stays as it was, at most s.
func (p *Prefix[S, Op]) Truncate(s int, kill bool) {
	p.ops, p.states = p.ops[:s], p.states[:s+1]
	if !kill {
		p.durable = s
	}
	p.durable = min(p.durable, s)
}

// Window is the prefixes a read that began at since (from Now) and ends
// now may have seen: lo is the number of writes up to the last one
// acknowledged before it began, hi every write invoked so far.
func (p *Prefix[S, Op]) Window(since int64) (lo, hi int) {
	for i, o := range p.ops {
		if o.ackedAt != 0 && o.ackedAt < since {
			lo = i + 1
		}
	}
	return lo, len(p.ops)
}

// LostDurableError is a recovered state that is the prefix of Recovered
// writes, short of the Floor that had to survive: the writes Lost (by
// name) were durable and are gone.
type LostDurableError struct {
	Recovered, Floor int
	Lost             []string
}

func (e *LostDurableError) Error() string {
	lost := e.Lost
	if len(lost) > 5 {
		lost = append(lost[:5:5], fmt.Sprintf("and %d more", len(e.Lost)-5))
	}
	return fmt.Sprintf("lost durable writes: the recovered state is the first %d writes, but the first %d were durable (lost: %s)",
		e.Recovered, e.Floor, strings.Join(lost, ", "))
}

// NotPrefixError is a recovered state that is no prefix of the writes
// (from Floor to Len): writes without earlier ones, or part of one.
// With Prefix.Diff set, Closest is the nearest prefix (After the write
// it ends with) and Diffs how the state differs from it; else Closest
// is -1.
type NotPrefixError struct {
	Floor, Len, Closest int
	After               string
	Diffs               []string
}

func (e *NotPrefixError) Error() string {
	msg := fmt.Sprintf("the recovered state is no prefix of the writes (from the first %d to all %d)", e.Floor, e.Len)
	if e.Closest >= 0 {
		after := "no writes"
		if e.Closest > 0 {
			after = fmt.Sprintf("the first %d, ending with %q", e.Closest, e.After)
		}
		d := e.Diffs
		if len(d) > 8 {
			d = append(d[:8:8], fmt.Sprintf("and %d more", len(e.Diffs)-8))
		}
		msg += fmt.Sprintf(": closest is %s: %s", after, strings.Join(d, "; "))
	}
	return msg
}
