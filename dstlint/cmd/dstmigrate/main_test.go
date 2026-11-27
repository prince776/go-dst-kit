package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"testing"
)

// The sample (every construct the tool rewrites) migrates to code that
// compiles and vets, with no raw channel operation, select, go statement
// or time.Now left. Fails if any survives or the result doesn't build.
func TestMigrateSample(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	kit := filepath.Join(filepath.Dir(here), "..", "..", "..")
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join(filepath.Dir(here), "testdata", "sample", "p.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	// The sample refers to dst already (the World field, dstRecv): give it
	// the import so it type-checks before migration.
	src = []byte(strings.Replace(string(src), "import (", "import (\n\t\"github.com/prince776/go-dst-kit/dst\"", 1))
	must := func(name string, args ...string) string {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
		return string(out)
	}
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/migr\n\ngo 1.25.0\n\nrequire github.com/prince776/go-dst-kit v0.0.0\n\nreplace github.com/prince776/go-dst-kit => "+kit+"\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "p.go"), src, 0o644)
	bin := filepath.Join(dir, "dstmigrate")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Dir(here)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	must(bin, "-world", "Pool=$.wd", ".")
	must("go", "vet", ".")
	got, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	for _, bad := range []string{`<-`, `select {`, `\bgo [a-z]`, `time\.(Now|Sleep|After)\(`, `make\(chan`, `\bchan\b`, `sync\.`, `rand\.Intn`, `context\.With`} {
		if regexp.MustCompile(bad).Match(got) {
			t.Errorf("%q survived:\n%s", bad, got)
		}
	}
	if !regexp.MustCompile(`// a comment inside a case\s+out\.Send`).Match(got) {
		t.Errorf("the comment isn't before its statement any more:\n%s", got)
	}
	t.Logf("\n%s", got)
}

// File I/O through package os (and io/ioutil) migrates to the World's FS:
// the result compiles and vets, no os filesystem call or *os.File is
// left, WriteString becomes io.WriteString, and a call with no World
// equivalent (os.Chmod) is reported, not silently kept.
// Fails if any os filesystem call or *os.File survives, the result
// doesn't build, or os.Chmod goes unreported.
func TestMigrateFiles(t *testing.T) {
	got, stderr := migrateSample(t, "files", "Store=$.w")
	for _, bad := range []string{`os\.(OpenFile|Open|Create|ReadFile|WriteFile|Rename|Remove|RemoveAll|Stat|Lstat|MkdirAll|Mkdir|ReadDir)\(`, `\*os\.File`, `ioutil\.WriteFile`, `\bf\.WriteString\(`} {
		if regexp.MustCompile(bad).MatchString(got) {
			t.Errorf("%q survived:\n%s", bad, got)
		}
	}
	for _, want := range []string{`s\.w\.FS\(\)\.Rename\(`, `dst\.Open\(s\.w\.FS\(\), `, `io\.WriteString\(f, "hello"\)`, `func size\(f dst\.File\)`, `os\.IsNotExist\(err\)`} {
		if !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("want %q in:\n%s", want, got)
		}
	}
	if !strings.Contains(stderr, "os.Chmod has no World equivalent") {
		t.Errorf("os.Chmod wasn't reported; stderr:\n%s", stderr)
	}
}

// migrateSample migrates testdata/<name>/p.go.txt in a scratch module with
// the -world mapping, checks the result vets, and returns it and the
// tool's stderr.
func migrateSample(t *testing.T, name, world string) (string, string) {
	t.Helper()
	_, here, _, _ := runtime.Caller(0)
	kit := filepath.Join(filepath.Dir(here), "..", "..", "..")
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join(filepath.Dir(here), "testdata", name, "p.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	src = []byte(strings.Replace(string(src), "import (", "import (\n\t\"github.com/prince776/go-dst-kit/dst\"", 1))
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/migr\n\ngo 1.25.0\n\nrequire github.com/prince776/go-dst-kit v0.0.0\n\nreplace github.com/prince776/go-dst-kit => "+kit+"\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "p.go"), src, 0o644)
	bin := filepath.Join(dir, "dstmigrate")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Dir(here)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "-world", world, ".")
	cmd.Dir = dir
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("dstmigrate: %v\n%s", err, stderr.String())
	}
	vet := exec.Command("go", "vet", ".")
	vet.Dir = dir
	got, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	if out, err := vet.CombinedOutput(); err != nil {
		t.Fatalf("vet: %v\n%s\n%s", err, out, got)
	}
	return string(got), stderr.String()
}
