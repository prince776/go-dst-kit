package check

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"math/rand/v2"
)

// testRand is a seeded generator for building histories.
type testRand struct{ *rand.Rand }

func newRand(seed uint64) *testRand {
	return &testRand{rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15))}
}
func (r *testRand) Intn(n int) int { return r.IntN(n) }
func (r *testRand) Bool() bool     { return r.IntN(2) == 0 }

type kvOp = Op[KVInput, KVOutput]

// bruteForce decides linearizability by trying every order of the
// operations that respects real time. Exponential: small histories only.
func bruteForce(m Model[KVState, KVInput, KVOutput], ops []kvOp) bool {
	n := len(ops)
	placed := make([]bool, n)
	precedes := func(a, b kvOp) bool { return !a.Unknown && a.Return < b.Call }
	var rec func(k int, s KVState) bool
	rec = func(k int, s KVState) bool {
		if k == n {
			return true
		}
		for i := range n {
			if placed[i] {
				continue
			}
			ok := true
			for j := range n {
				if !placed[j] && j != i && precedes(ops[j], ops[i]) {
					ok = false // an unplaced op must come before i
					break
				}
			}
			if !ok {
				continue
			}
			legal, next := m.Step(s, ops[i].Input, ops[i].Output, !ops[i].Unknown)
			if !legal {
				continue
			}
			placed[i] = true
			if rec(k+1, next) {
				placed[i] = false
				return true
			}
			placed[i] = false
		}
		return false
	}
	return rec(0, m.Init())
}

// realHistory runs n operations on one key, each taking effect at a point
// inside its interval, and records what clients would see. Some become
// unknown (the effect may or may not have happened).
func realHistory(r *testRand, n int, key string) []kvOp {
	type planned struct {
		op    kvOp
		point time.Duration
		takes bool // does an unknown op take effect?
	}
	ps := make([]planned, n)
	for i := range ps {
		call := time.Duration(r.Intn(20))
		ret := call + time.Duration(r.Intn(8))
		in := KVInput{Op: KVGet, Key: key}
		switch r.Intn(5) {
		case 0, 1:
			in = KVInput{Op: KVPut, Key: key, Value: fmt.Sprintf("v%d", i)}
		case 2:
			if r.Intn(4) == 0 {
				in = KVInput{Op: KVDelete, Key: key}
			}
		}
		ps[i] = planned{op: kvOp{Client: i, Input: in, Call: call, Return: ret}, point: call + time.Duration(r.Intn(int(ret-call)+1))}
		if r.Intn(6) == 0 {
			ps[i].op.Unknown = true
			ps[i].takes = r.Bool()
		}
	}
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int { return int(ps[a].point - ps[b].point) })
	var s KVState
	for _, i := range order {
		p := &ps[i]
		if p.op.Unknown && !p.takes {
			continue
		}
		_, next := KVModel().Step(s, p.op.Input, KVOutput{}, false)
		if p.op.Input.Op == KVGet {
			p.op.Output = KVOutput{Value: s.Value, Found: s.Found}
		}
		s = next
	}
	out := make([]kvOp, n)
	for i := range ps {
		out[i] = ps[i].op
		if out[i].Unknown {
			out[i].Return = 0
			out[i].Output = KVOutput{}
		}
	}
	return out
}

// corrupt changes one known get's output, which may or may not make the
// history non-linearizable.
func corrupt(r *testRand, ops []kvOp) []kvOp {
	ops = slices.Clone(ops)
	var gets []int
	for i, op := range ops {
		if op.Input.Op == KVGet && !op.Unknown {
			gets = append(gets, i)
		}
	}
	if len(gets) == 0 {
		return ops
	}
	i := gets[r.Intn(len(gets))]
	if r.Intn(4) == 0 {
		ops[i].Output = KVOutput{}
	} else {
		ops[i].Output = KVOutput{Value: fmt.Sprintf("v%d", r.Intn(len(ops))), Found: true}
	}
	return ops
}

func TestLaw_AcceptsRealExecutions(t *testing.T) {
	r := newRand(1)
	m := KVModel()
	for range 5000 {
		h := realHistory(r, 1+r.Intn(14), "k")
		if res := Linearizable(m, h, Options{}); res.Verdict != Pass {
			t.Fatalf("a real execution was judged %v:\n%s", res.Verdict, res.Explanation)
		}
	}
}

func TestLaw_MatchesBruteForce(t *testing.T) {
	r := newRand(2)
	m := KVModel()
	m.Partition = nil
	var pass, fail int
	for range 20000 {
		h := realHistory(r, 1+r.Intn(7), "k")
		if r.Bool() {
			h = corrupt(r, h)
		}
		got := Linearizable(m, h, Options{})
		want := bruteForce(m, h)
		if (got.Verdict == Pass) != want || got.Verdict == Inconclusive {
			t.Fatalf("checker says %v, brute force says linearizable=%v, for %v", got.Verdict, want, h)
		}
		if want {
			pass++
		} else {
			fail++
		}
	}
	if pass < 5000 || fail < 2000 {
		t.Fatalf("too few cases of one kind: %d linearizable, %d not", pass, fail)
	}
}

func TestLaw_PartitionsIndependently(t *testing.T) {
	r := newRand(3)
	m := KVModel()
	whole := m
	for range 3000 {
		var h []kvOp
		for k := range 1 + r.Intn(3) {
			part := realHistory(r, 1+r.Intn(6), fmt.Sprintf("k%d", k))
			if r.Intn(3) == 0 {
				part = corrupt(r, part)
			}
			h = append(h, part...)
		}
		r.Perm(len(h)) // interleave by shuffling
		perm := r.Perm(len(h))
		mixed := make([]kvOp, len(h))
		for i, p := range perm {
			mixed[i] = h[p]
		}
		got := Linearizable(whole, mixed, Options{}).Verdict == Pass
		want := true
		for k := range 3 {
			var only []kvOp
			for _, op := range mixed {
				if op.Input.Key == fmt.Sprintf("k%d", k) {
					only = append(only, op)
				}
			}
			single := m
			single.Partition = nil
			if Linearizable(single, only, Options{}).Verdict != Pass {
				want = false
			}
		}
		if got != want {
			t.Fatalf("partitioned verdict %v, per-key verdicts say %v", got, want)
		}
	}
}

func TestLaw_BudgetIsDeterministic(t *testing.T) {
	r := newRand(4)
	m := KVModel()
	for range 500 {
		h := corrupt(r, realHistory(r, 1+r.Intn(12), "k"))
		for _, max := range []int{0, 1, 5, 50} {
			a, b := Linearizable(m, h, Options{MaxSteps: max}), Linearizable(m, h, Options{MaxSteps: max})
			if a != b {
				t.Fatalf("two checks of one history differ: %+v vs %+v", a, b)
			}
		}
	}
	// A tiny budget really does stop the search.
	h := realHistory(newRand(9), 12, "k")
	if res := Linearizable(m, h, Options{MaxSteps: 1}); res.Verdict != Inconclusive {
		t.Fatalf("MaxSteps 1 gave %v, want inconclusive", res.Verdict)
	}
}

func TestViolationExplains(t *testing.T) {
	// put(x) completes, then a later get sees nothing: a stale read.
	h := []kvOp{
		{Client: 0, Input: KVInput{Op: KVPut, Key: "k", Value: "x"}, Call: 0, Return: 1},
		{Client: 1, Input: KVInput{Op: KVGet, Key: "k"}, Output: KVOutput{}, Call: 2, Return: 3},
	}
	res := Linearizable(KVModel(), h, Options{})
	if res.Verdict != Violation || res.Partition != "k" {
		t.Fatalf("got %+v", res)
	}
	for _, want := range []string{`get("k") -> <none>`, `put("k", "x")`, "places 1 of them"} {
		if !strings.Contains(res.Explanation, want) {
			t.Errorf("explanation lacks %q:\n%s", want, res.Explanation)
		}
	}
}

func TestLaw_KvSemantics(t *testing.T) {
	m := KVModel()
	s := m.Init()
	step := func(in KVInput, out KVOutput) bool {
		ok, next := m.Step(s, in, out, true)
		if ok {
			s = next
		}
		return ok
	}
	if !step(KVInput{Op: KVGet, Key: "a"}, KVOutput{}) || step(KVInput{Op: KVGet, Key: "a"}, KVOutput{Value: "", Found: true}) {
		t.Fatal("get on an empty key")
	}
	step(KVInput{Op: KVPut, Key: "a", Value: "1"}, KVOutput{})
	if !step(KVInput{Op: KVGet}, KVOutput{Value: "1", Found: true}) || step(KVInput{Op: KVGet}, KVOutput{Value: "2", Found: true}) || step(KVInput{Op: KVGet}, KVOutput{}) {
		t.Fatal("get after put")
	}
	step(KVInput{Op: KVDelete}, KVOutput{Value: "junk"})
	if !step(KVInput{Op: KVGet}, KVOutput{}) || step(KVInput{Op: KVGet}, KVOutput{Value: "1", Found: true}) {
		t.Fatal("get after delete")
	}
}

func TestLaw_KvUnknownGetsAreFree(t *testing.T) {
	m := KVModel()
	for _, s := range []KVState{{}, {Value: "x", Found: true}} {
		ok, next := m.Step(s, KVInput{Op: KVGet}, KVOutput{Value: "nonsense", Found: true}, false)
		if !ok || next != s {
			t.Fatalf("unknown get from %+v: legal=%v next=%+v", s, ok, next)
		}
	}
}

func newTestHistory() (*History[KVInput, KVOutput], *time.Duration) {
	var now time.Duration
	h := &History[KVInput, KVOutput]{now: func() time.Time { return time.Unix(0, 0).Add(now) }, base: time.Unix(0, 0)}
	return h, &now
}

func TestLaw_FirstEndingWins(t *testing.T) {
	h, now := newTestHistory()
	*now = 1
	a := h.Invoke(0, KVInput{Op: KVGet})
	*now = 2
	b := h.Invoke(1, KVInput{Op: KVGet})
	*now = 3
	c := h.Invoke(2, KVInput{Op: KVPut, Value: "v"})
	*now = 5
	if !a.Return(KVOutput{Value: "x", Found: true}) || a.Return(KVOutput{}) || a.Fail() || a.Abandon() {
		t.Fatal("a second ending of a was accepted")
	}
	if !b.Abandon() || b.Return(KVOutput{Found: true}) {
		t.Fatal("a reply after Abandon was accepted")
	}
	if !c.Fail() || c.Return(KVOutput{}) {
		t.Fatal("a reply after Fail was accepted")
	}
	ops := h.Ops()
	if len(ops) != 2 || ops[0].Output.Value != "x" || ops[0].Return != 5 || !ops[1].Unknown {
		t.Fatalf("ops %+v", ops)
	}
}

func TestLaw_UnfinishedIsUnknown(t *testing.T) {
	h, _ := newTestHistory()
	h.Invoke(0, KVInput{Op: KVPut, Value: "v"})
	if ops := h.Ops(); len(ops) != 1 || !ops[0].Unknown {
		t.Fatalf("ops %+v", ops)
	}
}

func TestLaw_FailedIsAbsent(t *testing.T) {
	h, _ := newTestHistory()
	h.Invoke(0, KVInput{Op: KVPut, Value: "v"}).Fail()
	if ops := h.Ops(); len(ops) != 0 {
		t.Fatalf("ops %+v", ops)
	}
}

// Err maps verdicts to errors a Final check can return.
func TestResultErr(t *testing.T) {
	if (Result{Verdict: Pass}).Err() != nil {
		t.Fatal("Pass has an error")
	}
	if err := (Result{Verdict: Violation, Explanation: "x"}).Err(); err == nil || !strings.Contains(err.Error(), "not linearizable") {
		t.Fatalf("Violation: %v", err)
	}
	if err := (Result{Verdict: Inconclusive}).Err(); !errors.Is(err, ErrInconclusive) {
		t.Fatalf("Inconclusive: %v", err)
	}
}
