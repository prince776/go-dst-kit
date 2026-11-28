package dsttest

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Verifies: [plants-caught-by-their-checks]
// Fails if: plantProblems accepts a plant's sweep that a check other than
// its own failed (another Always, another Final, a plain error such as a
// deadlock or a panic, a check name only mentioned in a message), that
// didn't fail, that never reached a switch, that names a check the setup
// doesn't register, or that was nondeterministic; or if it rejects a
// sweep its checks caught, the failures wrapped or not.
func TestPlantProblems(t *testing.T) {
	reg := map[string]bool{"logs match": true, "converged": true, "C1": true}
	inv := func(name string) error { return &InvariantError{Name: name, Err: errors.New("x")} }
	fin := func(name string) error { return &FinalError{Name: name, Err: errors.New("x")} }
	failed := func(errs ...error) Report {
		r := Report{Runs: 10}
		for i, e := range errs {
			r.Failed = append(r.Failed, Result{Seed: uint64(i + 1), Err: e})
		}
		return r
	}
	p := Plant{Bug: "skip-prev,flag-sticks", Checks: []string{"logs match", "converged"}}
	reached := []int64{3, 5}
	before := []int64{0, 0}
	for _, c := range []struct {
		name  string
		p     Plant
		rep   Report
		after []int64
		want  string // "" for no problem
	}{
		{"caught by both checks", p, failed(inv("logs match"), fin("converged")), reached, ""},
		{"caught, wrapped", p, failed(fmt.Errorf("seed 4: %w", inv("logs match"))), reached, ""},
		{"another invariant", p, failed(inv("logs match"), inv("C1")), reached, `seed 2 failed, but not by`},
		{"another final", p, failed(fin("C1")), reached, `seed 1 failed, but not by`},
		{"not a check", p, failed(errors.New("deadlock: every goroutine blocked")), reached, `seed 1 failed, but not by`},
		{"named only in the message", p, failed(errors.New(`invariant "logs match" broken`)), reached, `seed 1 failed, but not by`},
		{"no failure", p, failed(), reached, `no seed of 10 failed`},
		{"one switch never reached", p, failed(inv("logs match")), []int64{3, 0}, `never reached the switch "flag-sticks"`},
		{"unregistered check", Plant{Bug: "skip-prev", Checks: []string{"logs mtach"}}, failed(inv("logs mtach")), []int64{1}, `names check "logs mtach", which the setup doesn't register`},
		{"nondeterministic", p, Report{Runs: 10, Failed: failed(inv("logs match")).Failed, Nondeterministic: []Divergence{{Seed: 7}}}, reached, `seed 7 is nondeterministic`},
	} {
		got := plantProblems(c.p, c.rep, reg, before[:len(c.after)], c.after)
		switch {
		case c.want == "" && len(got) > 0:
			t.Errorf("%s: problems %q, want none", c.name, got)
		case c.want != "" && !strings.Contains(strings.Join(got, "\n"), c.want):
			t.Errorf("%s: problems %q, want one containing %q", c.name, got, c.want)
		}
	}
}

// Without the dstbugs tag no plant can be switched on: Plants skips,
// rather than passing a sweep that can't fail.
// Fails if Plants runs (or passes) in an untagged build.
func TestPlantsSkipsUntagged(t *testing.T) {
	if plantsTagged {
		t.Skip("the tagged build runs plants")
	}
	skipped := false
	t.Run("plants", func(t *testing.T) {
		defer func() { skipped = t.Skipped() }()
		Plants(t, Options{Seeds: 1}, func(h *Harness) {}, []Plant{{Bug: "x", Checks: []string{"c"}}})
	})
	if !skipped {
		t.Fatal("Plants ran in a build without the dstbugs tag")
	}
}

// TestPlantsTaggedBuild runs Plants end to end in a build with the
// dstbugs tag: a correct table passes; a table naming the wrong check,
// and a switch never reached, fails, saying so; a system failing with no
// plant fails the baseline.
// Fails if any goes the other way, or a reason is missing.
func TestPlantsTaggedBuild(t *testing.T) {
	if testing.Short() || plantsTagged {
		t.Skip()
	}
	run := func(name string) (string, error) {
		cmd := exec.Command("go", "test", "-count=1", "-tags", "dstbugs", "-run", "^"+name+"$", "-v", ".")
		cmd.Env = append(os.Environ(), "DSTTEST_PLANTS_MUST_FAIL=1")
		out, err := cmd.CombinedOutput()
		return string(out), err
	}
	out, err := run("TestPlantsTagged")
	if err != nil || !strings.Contains(out, "--- PASS: TestPlantsTagged/lost-update") || !strings.Contains(out, "--- PASS: TestPlantsTagged/goes-negative") || !strings.Contains(out, "--- PASS: TestPlantsTagged/none") {
		t.Fatalf("a correct table: %v\n%s", err, out)
	}
	out, err = run("TestPlantsTaggedBrokenBaseline")
	if err == nil || !strings.Contains(out, "--- FAIL: TestPlantsTaggedBrokenBaseline/none") || !strings.Contains(out, `failed with no plant: final check "broken anyway"`) {
		t.Errorf("a broken baseline wasn't reported: %v\n%s", err, out)
	}
	out, err = run("TestPlantsTaggedMisnamed")
	if err == nil {
		t.Errorf("a misnamed table passed")
	}
	for _, want := range []string{`lost-update: seed`, `failed, but not by ["never negative"]: final check "every add counted"`, `never reached the switch "never-placed"`, `--- PASS: TestPlantsTaggedMisnamed/none`} {
		if !strings.Contains(out, want) {
			t.Errorf("the misnamed table's output lacks %q:\n%s", want, out)
		}
	}
}
