package dsttest

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"hash/fnv"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

var (
	flagSave   = flag.Bool("dst.save", false, "save failing paths to the corpus (testdata/dst), and refresh drifted entries")
	flagCorpus = flag.Bool("dst.corpus", true, "replay the corpus before sweeping")
)

// corpusEntry is one saved path: a run that failed, kept so later runs of
// the test replay it first. It is a starting point, not a regression test:
// once the code changes the same path may play differently (it drifts),
// and a drifted entry that passes proves nothing about the bug it found.
type corpusEntry struct {
	Path        string   `json:"path"`
	Kept        string   `json:"kept"`  // why: the failure, its first line
	Saved       string   `json:"saved"` // the date it was saved or refreshed
	Trace       string   `json:"trace"` // the run's trace then: the same trace now is an exact replay
	Checkpoints []string `json:"checkpoints,omitempty"`
}

// corpusDir is where test's entries live: testdata/dst/<test name>, a
// subtest in a directory of its own.
func corpusDir(opts Options, test string) string {
	root := opts.Corpus
	if root == "" {
		root = filepath.Join("testdata", "dst")
	}
	return filepath.Join(append([]string{root}, strings.Split(test, "/")...)...)
}

func entryFile(dir, path string) string {
	h := fnv.New64a()
	h.Write([]byte(path))
	return filepath.Join(dir, fmt.Sprintf("%016x.json", h.Sum64()))
}

func loadCorpus(dir string) (map[string]corpusEntry, error) {
	out := map[string]corpusEntry{}
	files, err := os.ReadDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return out, nil
	}
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		if f.IsDir() || !strings.HasSuffix(f.Name(), ".json") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, f.Name()))
		if err != nil {
			return nil, err
		}
		var e corpusEntry
		if err := json.Unmarshal(data, &e); err != nil {
			return nil, fmt.Errorf("%s: %w", f.Name(), err)
		}
		out[filepath.Join(dir, f.Name())] = e
	}
	return out, nil
}

func newEntry(r Result) corpusEntry {
	e := corpusEntry{Path: r.Path.String(), Saved: time.Now().UTC().Format("2006-01-02"), Trace: fmt.Sprintf("%016x", r.Fingerprint)}
	if r.Err != nil {
		e.Kept = "failed: " + strings.SplitN(r.Err.Error(), "\n", 2)[0]
	}
	for _, c := range r.Checkpoints {
		e.Checkpoints = append(e.Checkpoints, fmt.Sprintf("%016x", c))
	}
	return e
}

func writeEntry(file string, e corpusEntry) error {
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}
	data, _ := json.MarshalIndent(e, "", "  ")
	return os.WriteFile(file, append(data, '\n'), 0o644)
}

// diverged is the first checkpoint step where r's run left the entry's,
// or "" if the checkpoints don't say.
func diverged(e corpusEntry, r Result, every uint64) string {
	for i, c := range r.Checkpoints {
		if i >= len(e.Checkpoints) || fmt.Sprintf("%016x", c) != e.Checkpoints[i] {
			if i == 0 {
				return "before step " + fmt.Sprint(every)
			}
			return fmt.Sprintf("between steps %d and %d", uint64(i)*every, uint64(i+1)*every)
		}
	}
	return ""
}

// replayCorpus replays test's saved entries: a failing one fails the test
// (exact, or drifted if the code changed since), a drifted one that passes
// is logged. With save, drifted entries get the current trace.
func replayCorpus(t interface {
	Errorf(string, ...any)
	Logf(string, ...any)
}, test string, opts Options, setup func(h *Harness), save bool) {
	dir := corpusDir(opts, test)
	entries, err := loadCorpus(dir)
	if err != nil {
		t.Errorf("dsttest: corpus: %v", err)
		return
	}
	for _, file := range slices.Sorted(mapsKeys(entries)) {
		e := entries[file]
		p, err := ParsePath(e.Path)
		if err != nil {
			t.Errorf("dsttest: corpus %s: %v", file, err)
			continue
		}
		r := RunPath(p, opts, setup, nil)
		exact := fmt.Sprintf("%016x", r.Fingerprint) == e.Trace
		how := "an exact replay"
		if !exact {
			how = "drifted: the code changed since it was saved " + e.Saved
			if d := diverged(e, r, 1000); d != "" {
				how += ", and the run left it " + d
			}
		}
		switch {
		case r.Err != nil:
			t.Errorf("corpus entry %s fails (%s): %v\nkept for: %s\nreplay: %s", e.Path, how, r.Err, e.Kept, p.flag())
		case !exact:
			t.Logf("corpus entry %s passes, but %s (not evidence the bug it found is fixed); -dst.save refreshes it", e.Path, how)
		default:
			t.Logf("corpus entry %s passes, %s: was the failure it found (%s) fixed only in the checks?", e.Path, how, e.Kept)
		}
		if save && !exact {
			ne := newEntry(r)
			ne.Kept = e.Kept
			if err := writeEntry(file, ne); err != nil {
				t.Errorf("dsttest: corpus: %v", err)
			}
		}
	}
}

// saveFailures adds the sweep's failing paths to the corpus (the first
// few, as Run reports them).
func saveFailures(t interface {
	Errorf(string, ...any)
	Logf(string, ...any)
}, test string, opts Options, rep Report) {
	dir := corpusDir(opts, test)
	for i, r := range rep.Failed {
		if i == 3 {
			break
		}
		file := entryFile(dir, r.Path.String())
		if _, err := os.Stat(file); err == nil {
			continue
		}
		if err := writeEntry(file, newEntry(r)); err != nil {
			t.Errorf("dsttest: corpus: %v", err)
			continue
		}
		t.Logf("saved %s to the corpus: %s", r.Path, file)
	}
}

func mapsKeys[V any](m map[string]V) func(func(string) bool) {
	return func(yield func(string) bool) {
		for k := range m {
			if !yield(k) {
				return
			}
		}
	}
}
