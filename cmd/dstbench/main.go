// Command dstbench measures how well simulation sweeps find known bugs.
//
// A target lists, in dstbench.json, bugs it can re-insert (by a switch
// compiled only under a build tag, selected by an environment variable)
// and the sweeps that should catch each. dstbench runs every (bug, sweep)
// cell with `go test -tags <tag> -dst.keepgoing -dst.json=...` and reports,
// per cell, the failure rate per seed (with a 95% Wilson interval), the
// seeds and CPU time to the first failure, and how often the bug's
// precondition was reached (a Sometimes marker, when the config names
// one). A baseline runs the sweeps with no bug: it must not fail.
//
// The benchmark is a floor for judging changes to exploration, not a
// target: bugs marked held_out are skipped unless -held-out is given, and
// are meant to be looked at only to check a change at the end.
//
// Usage, from the target's directory:
//
//	dstbench                      # every bug not held out, and the baseline
//	dstbench -seeds 1000 -bug lost-update
//	dstbench -held-out -out results.json
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

type config struct {
	Package  string   `json:"package"`  // the package holding the sweeps, e.g. "./src/svc"
	Tags     string   `json:"tags"`     // the build tag that compiles the switches in
	Env      string   `json:"env"`      // the variable naming the bug to re-insert
	Baseline []string `json:"baseline"` // sweeps run with no bug: they must pass
	Bugs     []bug    `json:"bugs"`
}

type bug struct {
	Name    string   `json:"name"`
	Tests   []string `json:"tests"`             // the sweeps to run it under
	Reached string   `json:"reached,omitempty"` // a Sometimes marker for its precondition
	HeldOut bool     `json:"held_out,omitempty"`
	Note    string   `json:"note,omitempty"`
}

type record struct {
	Seed     uint64          `json:"seed"`
	Index    int             `json:"index"`
	Failed   bool            `json:"failed"`
	Error    string          `json:"error"`
	WallNS   int64           `json:"wall_ns"`
	Coverage map[string]int  `json:"coverage"`
	Features map[string]bool `json:"features"`
}

// split is a cell's failures among seeds with a feature on and off.
type split struct{ FailOn, On, FailOff, Off int }

// cell is one (bug, sweep) measurement.
type cell struct {
	Bug, Test        string
	Seeds, Failures  int
	Rate, Low, High  float64 // failure rate per seed and its 95% interval
	FirstFailure     int     // seeds run up to and including the first failure (0: none)
	CPUToFirst       float64 // seconds of seed runs up to the first failure
	Reached          float64 // share of seeds that reached the precondition (-1: no marker)
	FirstError       string
	Wall             float64 // the cell's wall-clock seconds
	HeldOut          bool
	ExpectedFailures bool             // false for the baseline
	Features         map[string]split // per workload feature (Harness.Feature)
}

func main() {
	cfgPath := flag.String("config", "dstbench.json", "the target's benchmark config")
	seeds := flag.Int("seeds", 300, "seeds per cell")
	first := flag.String("first", "1", `first seed of every sweep (a number, or "random")`)
	heldOut := flag.Bool("held-out", false, "also run the held-out bugs")
	only := flag.String("bug", "", "run only this bug")
	onlyTest := flag.String("test", "", "run only this sweep")
	noBaseline := flag.Bool("no-baseline", false, "skip the baseline")
	out := flag.String("out", "", "also write the cells as JSON to this file")
	flag.Parse()

	data, err := os.ReadFile(*cfgPath)
	if err != nil {
		fatal(err)
	}
	var cfg config
	if err := json.Unmarshal(data, &cfg); err != nil {
		fatal(fmt.Errorf("%s: %w", *cfgPath, err))
	}
	dir := filepath.Dir(*cfgPath)
	if *first == "random" {
		*first = fmt.Sprint(1 + time.Now().UnixNano()%(1<<53))
		fmt.Printf("first seed %s\n", *first)
	}

	var cells []cell
	run := func(bugName, test, reached string, held, expect bool) {
		if *onlyTest != "" && test != *onlyTest {
			return
		}
		c, err := measure(dir, cfg, bugName, test, reached, *seeds, *first)
		if err != nil {
			fatal(fmt.Errorf("%s under %s: %w", orBaseline(bugName), test, err))
		}
		c.HeldOut, c.ExpectedFailures = held, expect
		cells = append(cells, c)
		fmt.Fprintf(os.Stderr, "  %-30s %-28s %4d/%d failed (%.1fs)\n", orBaseline(bugName), test, c.Failures, c.Seeds, c.Wall)
	}
	if !*noBaseline && *only == "" {
		for _, t := range cfg.Baseline {
			run("", t, "", false, false)
		}
	}
	for _, b := range cfg.Bugs {
		if (*only != "" && b.Name != *only) || (b.HeldOut && !*heldOut && *only != b.Name) {
			continue
		}
		for _, t := range b.Tests {
			run(b.Name, t, b.Reached, b.HeldOut, true)
		}
	}
	report(cells, *seeds)
	if *out != "" {
		b, _ := json.MarshalIndent(cells, "", "  ")
		if err := os.WriteFile(*out, b, 0o644); err != nil {
			fatal(err)
		}
	}
}

func orBaseline(name string) string {
	if name == "" {
		return "(baseline: no bug)"
	}
	return name
}

// measure runs one sweep with one bug re-inserted (none for "").
func measure(dir string, cfg config, bugName, test, reached string, seeds int, first string) (cell, error) {
	tmp, err := os.CreateTemp("", "dstbench-*.jsonl")
	if err != nil {
		return cell{}, err
	}
	tmp.Close()
	defer os.Remove(tmp.Name())
	args := []string{"test", "-count=1", "-timeout=2h", "-run", "^" + test + "$"}
	if cfg.Tags != "" {
		args = append(args, "-tags", cfg.Tags)
	}
	args = append(args, cfg.Package, "-dst.keepgoing", "-dst.json="+tmp.Name(),
		fmt.Sprintf("-dst.seeds=%d", seeds), "-dst.first="+first)
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), cfg.Env+"="+bugName)
	var outBuf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &outBuf, &outBuf
	start := time.Now()
	runErr := cmd.Run() // a failing sweep exits non-zero: the records say why
	wall := time.Since(start).Seconds()

	f, err := os.Open(tmp.Name())
	if err != nil {
		return cell{}, err
	}
	defer f.Close()
	var recs []record
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<20), 1<<26)
	for sc.Scan() {
		var r record
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			return cell{}, err
		}
		recs = append(recs, r)
	}
	if len(recs) == 0 {
		tail := outBuf.String()
		if len(tail) > 3000 {
			tail = tail[len(tail)-3000:]
		}
		return cell{}, fmt.Errorf("no seed records (%v):\n%s", runErr, tail)
	}
	sort.Slice(recs, func(i, j int) bool { return recs[i].Index < recs[j].Index })
	c := cell{Bug: bugName, Test: test, Seeds: len(recs), Wall: wall, Reached: -1}
	reachedN := 0
	cpu := 0.0
	for i, r := range recs {
		cpu += float64(r.WallNS) / 1e9
		if r.Failed {
			c.Failures++
			if c.FirstFailure == 0 {
				c.FirstFailure, c.CPUToFirst, c.FirstError = i+1, cpu, r.Error
			}
		}
		if reached != "" && r.Coverage[reached] > 0 {
			reachedN++
		}
		for f, on := range r.Features {
			if c.Features == nil {
				c.Features = map[string]split{}
			}
			sp := c.Features[f]
			if on {
				sp.On++
				if r.Failed {
					sp.FailOn++
				}
			} else {
				sp.Off++
				if r.Failed {
					sp.FailOff++
				}
			}
			c.Features[f] = sp
		}
	}
	if reached != "" {
		c.Reached = float64(reachedN) / float64(len(recs))
	}
	c.Rate = float64(c.Failures) / float64(c.Seeds)
	c.Low, c.High = wilson(c.Failures, c.Seeds)
	return c, nil
}

// wilson is the 95% Wilson score interval for k successes in n trials.
func wilson(k, n int) (float64, float64) {
	if n == 0 {
		return 0, 1
	}
	const z = 1.96
	p, nf := float64(k)/float64(n), float64(n)
	d := 1 + z*z/nf
	mid := (p + z*z/(2*nf)) / d
	half := z * math.Sqrt(p*(1-p)/nf+z*z/(4*nf*nf)) / d
	return math.Max(0, mid-half), math.Min(1, mid+half)
}

func report(cells []cell, seeds int) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "bug\tsweep\tfailed\trate per seed (95%)\tfirst failure\tCPU to first\treached\t")
	found, total, bad := 0, 0, 0
	for _, c := range cells {
		name := orBaseline(c.Bug)
		if c.HeldOut {
			name += " (held out)"
		}
		ff, cpu := "never", "-"
		if c.FirstFailure > 0 {
			ff, cpu = fmt.Sprintf("seed #%d", c.FirstFailure), fmt.Sprintf("%.1fs", c.CPUToFirst)
		}
		reached := "-"
		if c.Reached >= 0 {
			reached = fmt.Sprintf("%.0f%%", 100*c.Reached)
		}
		fmt.Fprintf(w, "%s\t%s\t%d/%d\t%.1f%% [%.1f, %.1f]\t%s\t%s\t%s\t\n", name, c.Test, c.Failures, c.Seeds,
			100*c.Rate, 100*c.Low, 100*c.High, ff, cpu, reached)
		if !c.ExpectedFailures && c.Failures > 0 {
			bad++
		}
	}
	w.Flush()
	// Features that change a bug's rate a lot: one that hides a bug needs
	// the seeds without it; one that exposes it, a larger share of seeds.
	var notes []string
	for _, c := range cells {
		for _, f := range sortedKeys(c.Features) {
			sp := c.Features[f]
			if sp.On == 0 || sp.Off == 0 || sp.FailOn+sp.FailOff < 3 {
				continue
			}
			on, off := float64(sp.FailOn)/float64(sp.On), float64(sp.FailOff)/float64(sp.Off)
			lo, hi := math.Min(on, off), math.Max(on, off)
			if hi >= 3*lo { // 3x either way (or one side never fails)
				notes = append(notes, fmt.Sprintf("  %s under %s: with %s on %d/%d failed, off %d/%d",
					orBaseline(c.Bug), c.Test, f, sp.FailOn, sp.On, sp.FailOff, sp.Off))
			}
		}
	}
	if len(notes) > 0 {
		fmt.Println("\nworkload features that change a bug's rate 3x or more:")
		fmt.Println(strings.Join(notes, "\n"))
	}
	byBug := map[string]bool{}
	for _, c := range cells {
		if c.ExpectedFailures {
			if _, ok := byBug[c.Bug]; !ok {
				byBug[c.Bug] = false
				total++
			}
			if c.Failures > 0 && !byBug[c.Bug] {
				byBug[c.Bug] = true
				found++
			}
		}
	}
	fmt.Printf("\nfound %d of %d bugs in %d seeds per sweep", found, total, seeds)
	if bad > 0 {
		fmt.Printf("; the baseline FAILED in %d sweeps (a false positive or an unfixed bug)", bad)
	}
	fmt.Println()
	var missed []string
	for b, ok := range byBug {
		if !ok {
			missed = append(missed, b)
		}
	}
	sort.Strings(missed)
	if len(missed) > 0 {
		fmt.Printf("never found: %s\n", strings.Join(missed, ", "))
	}
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "dstbench:", err)
	os.Exit(1)
}
