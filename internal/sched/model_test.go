package sched_test

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/sched"
	"github.com/prince776/go-dst-kit/internal/simworld"
)

// ---- small channel programs ----

type opKind int

const (
	opSend opKind = iota
	opRecv
	opClose
	opSelect
)

type arm struct {
	send bool
	ch   int
	val  int
}

type op struct {
	kind   opKind
	ch     int
	val    int
	arms   []arm
	hasDef bool
}

type program struct {
	caps []int
	g    [][]op
}

func (p program) String() string {
	var b strings.Builder
	fmt.Fprintf(&b, "caps %v;", p.caps)
	for i, ops := range p.g {
		fmt.Fprintf(&b, " g%d:", i)
		for _, o := range ops {
			switch o.kind {
			case opSend:
				fmt.Fprintf(&b, " c%d<-%d", o.ch, o.val)
			case opRecv:
				fmt.Fprintf(&b, " <-c%d", o.ch)
			case opClose:
				fmt.Fprintf(&b, " close(c%d)", o.ch)
			case opSelect:
				b.WriteString(" select{")
				for _, a := range o.arms {
					if a.send {
						fmt.Fprintf(&b, "c%d<-%d,", a.ch, a.val)
					} else {
						fmt.Fprintf(&b, "<-c%d,", a.ch)
					}
				}
				if o.hasDef {
					b.WriteString("default")
				}
				b.WriteString("}")
			}
		}
	}
	return b.String()
}

func genProgram(r *rand.Rand) program {
	nc := 1 + r.IntN(2)
	p := program{caps: make([]int, nc)}
	for i := range p.caps {
		p.caps[i] = r.IntN(3)
	}
	val := 1
	for range 2 + r.IntN(2) {
		var ops []op
		for range 1 + r.IntN(3) {
			ch := r.IntN(nc)
			switch k := r.IntN(10); {
			case k < 4:
				ops = append(ops, op{kind: opSend, ch: ch, val: val})
				val++
			case k < 8:
				ops = append(ops, op{kind: opRecv, ch: ch})
			case k < 9 && r.IntN(3) == 0:
				ops = append(ops, op{kind: opClose, ch: ch})
			default:
				o := op{kind: opSelect, hasDef: r.IntN(3) == 0}
				for range 1 + r.IntN(2) {
					a := arm{send: r.IntN(2) == 0, ch: r.IntN(nc), val: val}
					val++
					o.arms = append(o.arms, a)
				}
				ops = append(ops, o)
			}
		}
		p.g = append(p.g, ops)
	}
	return p
}

// ---- the independent model: every interleaving of Go's semantics ----
//
// Each goroutine runs its operations one at a time. An operation that can
// complete does so atomically; one that can't blocks, queued FIFO on its
// channel(s). A blocked goroutine that gets a value (or a close) is woken
// and records the result on its next step, as a real goroutine resumes.

type waiter struct {
	g, arm int // arm -1 for a plain send/receive
	val    int
	seq    int // the goroutine's wait number; stale entries are dead
}

type chanState struct {
	buf          []int
	closed       bool
	recvq, sendq []waiter
}

type gState struct {
	pc      int
	obs     []string
	blocked bool
	seq     int
	pending string // result to record on wake ("" none)
	woken   bool
}

type mstate struct {
	ch []chanState
	gs []gState
}

func (s mstate) clone() mstate {
	c := mstate{ch: make([]chanState, len(s.ch)), gs: make([]gState, len(s.gs))}
	for i, x := range s.ch {
		c.ch[i] = chanState{buf: slices.Clone(x.buf), closed: x.closed, recvq: slices.Clone(x.recvq), sendq: slices.Clone(x.sendq)}
	}
	for i, g := range s.gs {
		g.obs = slices.Clone(g.obs)
		c.gs[i] = g
	}
	return c
}

func (s mstate) key() string { return fmt.Sprintf("%v", s) }

func (s *mstate) alive(w waiter) bool {
	g := s.gs[w.g]
	return g.blocked && !g.woken && g.seq == w.seq
}

func (s *mstate) popAlive(q *[]waiter) (waiter, bool) {
	for len(*q) > 0 {
		w := (*q)[0]
		*q = (*q)[1:]
		if s.alive(w) {
			return w, true
		}
	}
	return waiter{}, false
}

func (s *mstate) hasAlive(q []waiter) bool {
	for _, w := range q {
		if s.alive(w) {
			return true
		}
	}
	return false
}

func (s *mstate) wake(w waiter, result string) {
	g := &s.gs[w.g]
	g.woken = true
	g.pending = result
}

func recvResult(arm, v int, ok bool) string {
	r := fmt.Sprintf("recv %d", v)
	if !ok {
		r = "recv closed"
	}
	if arm >= 0 {
		return fmt.Sprintf("sel%d %s", arm, r)
	}
	return r
}

func sendResult(arm int) string {
	if arm >= 0 {
		return fmt.Sprintf("sel%d sent", arm)
	}
	return "sent"
}

// trySend performs a send if it can complete now (ok), or reports a panic.
func (s *mstate) trySend(ch, v int) (ok bool, panicMsg string) {
	c := &s.ch[ch]
	if c.closed {
		return false, "send on closed channel"
	}
	if r, found := s.popAlive(&c.recvq); found {
		s.wake(r, recvResult(r.arm, v, true))
		return true, ""
	}
	if len(c.buf) < cap0(c, s, ch) {
		c.buf = append(c.buf, v)
		return true, ""
	}
	return false, ""
}

var capsOf []int // set per program by explore

func cap0(_ *chanState, _ *mstate, ch int) int { return capsOf[ch] }

func (s *mstate) tryRecv(ch int) (v int, ok, done bool) {
	c := &s.ch[ch]
	if len(c.buf) > 0 {
		v, c.buf = c.buf[0], c.buf[1:]
		if w, found := s.popAlive(&c.sendq); found {
			c.buf = append(c.buf, w.val)
			s.wake(w, sendResult(w.arm))
		}
		return v, true, true
	}
	if w, found := s.popAlive(&c.sendq); found {
		s.wake(w, sendResult(w.arm))
		return w.val, true, true
	}
	if c.closed {
		return 0, false, true
	}
	return 0, false, false
}

func (s *mstate) sendReady(ch int) bool {
	c := &s.ch[ch]
	return c.closed || s.hasAlive(c.recvq) || len(c.buf) < capsOf[ch]
}

func (s *mstate) recvReady(ch int) bool {
	c := &s.ch[ch]
	return len(c.buf) > 0 || s.hasAlive(c.sendq) || c.closed
}

func outcome(status string, gs []gState) string {
	var parts []string
	for i, g := range gs {
		parts = append(parts, fmt.Sprintf("g%d[%s]", i, strings.Join(g.obs, ",")))
	}
	return status + " " + strings.Join(parts, " ")
}

// explore returns every outcome the model allows.
func explore(p program) map[string]bool {
	capsOf = p.caps
	out := map[string]bool{}
	seen := map[string]bool{}
	var dfs func(s mstate)
	dfs = func(s mstate) {
		k := s.key()
		if seen[k] {
			return
		}
		seen[k] = true
		moved := false
		for gi := range s.gs {
			g := s.gs[gi]
			if g.pc >= len(p.g[gi]) {
				continue
			}
			if g.blocked && !g.woken {
				continue
			}
			o := p.g[gi][g.pc]
			if g.woken { // resume: record the result
				n := s.clone()
				ng := &n.gs[gi]
				if ng.pending == "panic" {
					out[outcome("panic: send on closed channel", n.gs)] = true
					moved = true
					continue
				}
				if ng.pending != "sent" {
					ng.obs = append(ng.obs, ng.pending)
				}
				ng.pending, ng.woken, ng.blocked = "", false, false
				ng.pc++
				dfs(n)
				moved = true
				continue
			}
			for _, n := range s.steps(gi, o) {
				moved = true
				if strings.HasPrefix(n.panic, "panic") {
					out[outcome(n.panic, n.s.gs)] = true
					continue
				}
				dfs(n.s)
			}
		}
		if !moved {
			status := "ok"
			for gi, g := range s.gs {
				if g.pc < len(p.g[gi]) {
					status = "deadlock"
				}
			}
			out[outcome(status, s.gs)] = true
		}
	}
	init := mstate{ch: make([]chanState, len(p.caps)), gs: make([]gState, len(p.g))}
	dfs(init)
	return out
}

type next struct {
	s     mstate
	panic string
}

// steps lists the states after goroutine gi starts operation o.
func (s mstate) steps(gi int, o op) []next {
	block := func(n mstate, enqueue func(n *mstate, w waiter)) next {
		g := &n.gs[gi]
		g.seq++
		g.blocked = true
		enqueue(&n, waiter{g: gi, arm: -1, seq: g.seq})
		return next{s: n}
	}
	switch o.kind {
	case opSend:
		n := s.clone()
		ok, pm := n.trySend(o.ch, o.val)
		if pm != "" {
			return []next{{s: n, panic: "panic: " + pm}}
		}
		if ok {
			n.gs[gi].pc++
			return []next{{s: n}}
		}
		return []next{block(n, func(n *mstate, w waiter) {
			w.val = o.val
			n.ch[o.ch].sendq = append(n.ch[o.ch].sendq, w)
		})}
	case opRecv:
		n := s.clone()
		if v, ok, done := n.tryRecv(o.ch); done {
			n.gs[gi].obs = append(n.gs[gi].obs, recvResult(-1, v, ok))
			n.gs[gi].pc++
			return []next{{s: n}}
		}
		return []next{block(n, func(n *mstate, w waiter) { n.ch[o.ch].recvq = append(n.ch[o.ch].recvq, w) })}
	case opClose:
		n := s.clone()
		c := &n.ch[o.ch]
		if c.closed {
			return []next{{s: n, panic: "panic: close of closed channel"}}
		}
		c.closed = true
		for w, ok := n.popAlive(&c.recvq); ok; w, ok = n.popAlive(&c.recvq) {
			n.wake(w, recvResult(w.arm, 0, false))
		}
		for w, ok := n.popAlive(&c.sendq); ok; w, ok = n.popAlive(&c.sendq) {
			n.wake(w, "panic")
		}
		n.gs[gi].pc++
		return []next{{s: n}}
	default: // select
		var res []next
		for ai, a := range o.arms {
			ready := s.recvReady(a.ch)
			if a.send {
				ready = s.sendReady(a.ch)
			}
			if !ready {
				continue
			}
			n := s.clone()
			if a.send {
				ok, pm := n.trySend(a.ch, a.val)
				if pm != "" {
					res = append(res, next{s: n, panic: "panic: " + pm})
					continue
				}
				_ = ok
				n.gs[gi].obs = append(n.gs[gi].obs, sendResult(ai))
			} else {
				v, ok, _ := n.tryRecv(a.ch)
				n.gs[gi].obs = append(n.gs[gi].obs, recvResult(ai, v, ok))
			}
			n.gs[gi].pc++
			res = append(res, next{s: n})
		}
		if len(res) > 0 {
			return res
		}
		n := s.clone()
		if o.hasDef {
			n.gs[gi].obs = append(n.gs[gi].obs, "default")
			n.gs[gi].pc++
			return []next{{s: n}}
		}
		g := &n.gs[gi]
		g.seq++
		g.blocked = true
		for ai, a := range o.arms {
			w := waiter{g: gi, arm: ai, val: a.val, seq: g.seq}
			if a.send {
				n.ch[a.ch].sendq = append(n.ch[a.ch].sendq, w)
			} else {
				n.ch[a.ch].recvq = append(n.ch[a.ch].recvq, w)
			}
		}
		return []next{{s: n}}
	}
}

// ---- the same program on the simulated World ----

func runSim(p program, seed uint64) string { return runSimWith(p, simworld.Options{Seed: seed}) }

func runSimWith(p program, opts simworld.Options) string {
	obs := make([][]string, len(p.g))
	res := simworld.Run(opts, func(w *dst.World) {
		chans := make([]dst.Chan[int], len(p.caps))
		for i, c := range p.caps {
			chans[i] = dst.MakeChan[int](w, c)
		}
		var wg dst.WaitGroup
		for gi, ops := range p.g {
			wg.Add(1)
			w.Go(func() {
				defer wg.Done()
				for _, o := range ops {
					switch o.kind {
					case opSend:
						chans[o.ch].Send(o.val)
					case opRecv:
						v, ok := chans[o.ch].Recv()
						obs[gi] = append(obs[gi], recvResult(-1, v, ok))
					case opClose:
						chans[o.ch].Close()
					case opSelect:
						var cases []dst.Case
						for ai, a := range o.arms {
							if a.send {
								cases = append(cases, dst.OnSend(chans[a.ch], a.val, func() { obs[gi] = append(obs[gi], sendResult(ai)) }))
							} else {
								cases = append(cases, dst.OnRecv(chans[a.ch], func(v int, ok bool) { obs[gi] = append(obs[gi], recvResult(ai, v, ok)) }))
							}
						}
						if o.hasDef {
							cases = append(cases, dst.Default(func() { obs[gi] = append(obs[gi], "default") }))
						}
						dst.Select(w, cases...)
					}
				}
			})
		}
		wg.Wait()
	})
	status := "ok"
	var de *sched.DeadlockError
	var pe *sched.PanicError
	switch {
	case res.Err == nil:
	case errors.As(res.Err, &de):
		status = "deadlock"
	case errors.As(res.Err, &pe):
		status = fmt.Sprintf("panic: %v", pe.Value)
	default:
		status = "error: " + res.Err.Error()
	}
	gs := make([]gState, len(p.g))
	for i := range gs {
		gs[i].obs = obs[i]
	}
	return outcome(status, gs)
}

// Verifies: [chan-matches-go], [deadlock-reported], [panic-reported]
// Fails if: the scheduler produces an outcome Go can't (unsound), or misses
// one Go can produce (incomplete), for any of the generated programs.
func TestChanMatchesGo(t *testing.T) {
	r := rand.New(rand.NewPCG(1, 2))
	programs, explored := 0, 0
	for range 400 {
		p := genProgram(r)
		want := explore(p)
		programs++
		if len(want) > 40 {
			continue // too many outcomes to cover by sampling seeds
		}
		explored++
		got := map[string]bool{}
		for seed := uint64(1); seed <= 30000 && len(got) < len(want); seed++ {
			o := runSim(p, seed)
			if !want[o] {
				t.Fatalf("program %v\nthe simulator produced %q (seed %d), which Go can't; Go allows:\n  %s", p, o, seed, strings.Join(sortedKeys(want), "\n  "))
			}
			got[o] = true
		}
		for o := range want {
			if !got[o] {
				t.Fatalf("program %v\nGo allows %q, which 30000 seeds never produced; seen:\n  %s", p, o, strings.Join(sortedKeys(got), "\n  "))
			}
		}
	}
	if explored < 300 {
		t.Fatalf("only %d of %d programs were small enough to cover", explored, programs)
	}
	t.Logf("%d programs, every outcome matched in both directions", explored)
}

// The comparison must be able to fail: a scheduler that never picked the
// second ready arm of a select would miss outcomes, and the model would
// show it. Here we check the model itself distinguishes such programs.
func TestModelSeesSelectChoice(t *testing.T) {
	p := program{caps: []int{1, 1}, g: [][]op{
		{{kind: opSend, ch: 0, val: 1}, {kind: opSend, ch: 1, val: 2},
			{kind: opSelect, arms: []arm{{ch: 0}, {ch: 1}}}},
	}}
	want := explore(p)
	if len(want) != 2 {
		t.Fatalf("model outcomes %v, want both select arms", sortedKeys(want))
	}
}

func sortedKeys(m map[string]bool) []string {
	k := make([]string, 0, len(m))
	for x := range m {
		k = append(k, x)
	}
	slices.Sort(k)
	return k
}
