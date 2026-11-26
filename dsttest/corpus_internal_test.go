package dsttest

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

type fakeT struct{ errs, logs []string }

func (f *fakeT) Helper() {}
func (f *fakeT) Errorf(format string, args ...any) {
	f.errs = append(f.errs, fmt.Sprintf(format, args...))
}
func (f *fakeT) Logf(format string, args ...any) {
	f.logs = append(f.logs, fmt.Sprintf(format, args...))
}

// A corpus entry's life: saved when a sweep fails; replayed as an exact
// failure while the code is unchanged; as a drifted failure once the code
// changes; logged (never called fixed) once a drifted run passes; and
// exact again after -dst.save refreshes it. Fails if any step reports the
// wrong thing.
func TestCorpus(t *testing.T) {
	// setup: goroutines counting; buggy always fails (this tests how
	// replays are reported, not whether a bug survives a change); changed
	// adds a goroutine, so saved paths drift.
	setup := func(buggy, changed bool) func(h *Harness) {
		return func(h *Harness) {
			n := 0
			for g := range 2 + map[bool]int{false: 0, true: 1}[changed] {
				h.Go(func(w *dst.World) {
					for range 20 {
						n++
						w.Sleep(time.Duration(1+w.Rand().IntN(3)) * time.Millisecond)
					}
					if buggy && g == 1 {
						h.Fail(fmt.Errorf("count %d", n))
					}
				})
			}
		}
	}
	opts := Options{Seeds: 40, FaultPhase: 100 * time.Millisecond, RecoveryPhase: 100 * time.Millisecond, Corpus: t.TempDir(), KeepGoing: true}
	rep := Sweep(opts, setup(true, false))
	if len(rep.Failed) == 0 {
		t.Fatal("the bug wasn't found")
	}
	ft := &fakeT{}
	saveFailures(ft, "TestX/sub", opts, rep)
	files, _ := os.ReadDir(corpusDir(opts, "TestX/sub"))
	if len(files) != min(3, len(rep.Failed)) || len(ft.errs) > 0 {
		t.Fatalf("saved %d entries (errors %v)", len(files), ft.errs)
	}

	check := func(name string, buggy, changed, save bool, wantErr, wantLog string) {
		t.Helper()
		ft := &fakeT{}
		replayCorpus(ft, "TestX/sub", opts, setup(buggy, changed), save)
		got := strings.Join(ft.errs, "\n")
		if wantErr == "" && got != "" || wantErr != "" && (len(ft.errs) != len(files) || !strings.Contains(got, wantErr)) {
			t.Errorf("%s: errors %q, want %d mentioning %q", name, got, len(files), wantErr)
		}
		if logs := strings.Join(ft.logs, "\n"); wantLog != "" && !strings.Contains(logs, wantLog) {
			t.Errorf("%s: logs %q, want %q", name, logs, wantLog)
		}
	}
	check("unchanged", true, false, false, "fails (an exact replay)", "")
	check("changed", true, true, false, "fails (drifted", "")
	check("fixed", false, true, false, "", "passes, but drifted")
	check("refreshed", false, true, true, "", "")
	check("after refresh", false, true, false, "", "passes, an exact replay")
}
