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
// or time.Now left, and a nil channel argument becomes the zero dst.Chan
// without moving the comment after it. Fails if any survives, the result
// doesn't build, or the nil argument is kept or moves the comment.
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
	if !regexp.MustCompile(`notifyAll\(1, dst\.Chan\[struct\{\}\]\{\}\)\s*\}\s*// notifyAll's comment stays above it\.\s*func notifyAll`).Match(got) {
		t.Errorf("a nil channel argument didn't become the zero dst.Chan, or moved a comment:\n%s", got)
	}
	if !regexp.MustCompile(`// a comment inside a case\s+out\.Send`).Match(got) {
		t.Errorf("the comment isn't before its statement any more:\n%s", got)
	}
	t.Logf("\n%s", got)
}

// File I/O through package os (and io/ioutil) migrates to the World's FS:
// the result compiles and vets, no os filesystem call or *os.File is
// left, WriteString becomes io.WriteString, and a call with no World
// equivalent (os.Chmod) is reported, not silently kept; comments stay
// where they were.
// Fails if any os filesystem call or *os.File survives, the result
// doesn't build, os.Chmod goes unreported, or a rewritten call moves the
// comment after it.
func TestMigrateFiles(t *testing.T) {
	got, stderr := migrateSample(t, "files", "Store=$.w")
	for _, bad := range []string{`os\.(OpenFile|Open|Create|ReadFile|WriteFile|Rename|Remove|RemoveAll|Stat|Lstat|MkdirAll|Mkdir|ReadDir)\(`, `\*os\.File`, `ioutil\.WriteFile`, `\bf\.WriteString\(`} {
		if regexp.MustCompile(bad).MatchString(got) {
			t.Errorf("%q survived:\n%s", bad, got)
		}
	}
	for _, want := range []string{`s\.w\.FS\(\)\.Rename\(`, `dst\.Open\(s\.w\.FS\(\), `, `io\.WriteString\(f, "hello"\)`, `func size\(f dst\.File\)`, `os\.IsNotExist\(err\)`,
		`\n// size takes a file: its type becomes dst\.File\.\nfunc size`} {
		if !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("want %q in:\n%s", want, got)
		}
	}
	if !strings.Contains(stderr, "os.Chmod has no World equivalent") {
		t.Errorf("os.Chmod wasn't reported; stderr:\n%s", stderr)
	}
}

// The same with a World expression that is a call (s.world()): the
// rewritten calls nest deeper, and comments must still stay put, after
// a file call (drop) and a time call (age) alike.
// Fails if the comment before age or size moves.
func TestMigrateFilesWorldCall(t *testing.T) {
	got, _ := migrateSample(t, "files", "Store=$.world()")
	for _, want := range []string{`\n// age is how long ago t was\.\nfunc`, `\n// size takes a file: its type becomes dst\.File\.\nfunc size`} {
		if !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("a comment moved (want %q):\n%s", want, got)
		}
	}
}

// A file named by -real-fs keeps its os file I/O: it uses the real
// filesystem on purpose (a test's temporary directory).
// Fails if any of the sample's os calls or *os.File is rewritten, or
// os.Chmod is reported.
func TestMigrateFilesRealFS(t *testing.T) {
	got, stderr := migrateSample(t, "files", "Store=$.w", "-real-fs", "x.go,p.go")
	for _, want := range []string{`os\.Rename\(`, `os\.Open\(`, `\*os\.File`, `f\.WriteString\(`} {
		if !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("want %q kept in:\n%s", want, got)
		}
	}
	if strings.Contains(stderr, "no World equivalent") {
		t.Errorf("a -real-fs file's call was reported; stderr:\n%s", stderr)
	}
}

// migrateSample migrates testdata/<name>/p.go.txt in a scratch module with
// the -world mapping, checks the result vets, and returns it and the
// tool's stderr.
func migrateSample(t *testing.T, name, world string, flags ...string) (string, string) {
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
	cmd := exec.Command(bin, append(append([]string{"-world", world}, flags...), ".")...)
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
