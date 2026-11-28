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
// or time.Now left, a nil channel in a literal compiles, and a nil channel
// argument becomes the zero dst.Chan
// without moving the comment after it; math/rand/v2's N and
// crypto/rand.Reader become the World's, runtime.Gosched becomes
// w.Yield, and a rand function with no
// World equivalent is reported. Fails if any survives, the result doesn't
// build, the nil argument is kept or moves the comment, or the unmapped
// rand call goes unreported.
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
	warnings := must(bin, "-world", "Pool=$.wd", ".")
	must("go", "vet", ".")
	got, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	if !strings.Contains(warnings, "rand.NormFloat64 has no World equivalent") {
		t.Errorf("rand.NormFloat64 kept without a warning:\n%s", warnings)
	}
	for _, bad := range []string{`<-`, `select {`, `\bgo [a-z]`, `time\.(Now|Sleep|After)\(`, `make\(chan`, `\bchan\b`, `sync\.`, `rand\.Intn`, `mrand\.N\(`, `crand\.`, `context\.With`, `runtime\.Gosched`} {
		if regexp.MustCompile(bad).Match(got) {
			t.Errorf("%q survived:\n%s", bad, got)
		}
	}
	if !regexp.MustCompile(`notifyAll\(1, dst\.Chan\[struct\{\}\]\{\}\)\s*\}\s*// notifyAll's comment stays above it\.\s*func notifyAll`).Match(got) {
		t.Errorf("a nil channel argument didn't become the zero dst.Chan, or moved a comment:\n%s", got)
	}
	if !regexp.MustCompile(`wg\.Go\(p\.wd, func\(\) \{ p\.worker\(1\) \}\)`).Match(got) {
		t.Errorf("WaitGroup.Go didn't get the World:\n%s", got)
	}
	if !regexp.MustCompile(`for !flag\.Load\(\) \{\s+p\.wd\.Yield\(\)`).Match(got) {
		t.Errorf("runtime.Gosched didn't become the World's Yield:\n%s", got)
	}
	if !regexp.MustCompile(`// a comment inside a case\s+out\.Send`).Match(got) {
		t.Errorf("the comment isn't before its statement any more:\n%s", got)
	}
	t.Logf("\n%s", got)
}

// A go statement's operands are evaluated at the go statement: the
// migrated program prints what the original does when every operand
// changes right after its go statement (a variable, a value receiver,
// a value method through a pointer, a pointer, a func variable, a func
// field), and an operand that never changes isn't bound.
// Fails if any goroutine sees a value from after its go statement, or a
// stable operand gets a binding.
func TestMigrateGoArgsEvaluatedNow(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	kit := filepath.Join(filepath.Dir(here), "..", "..", "..")
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join(filepath.Dir(here), "testdata", "goargs", "p.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
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
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/goargs\n\ngo 1.25.0\n\nrequire github.com/prince776/go-dst-kit v0.0.0\n\nreplace github.com/prince776/go-dst-kit => "+kit+"\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "p.go"), src, 0o644)
	want := must("go", "run", ".")
	bin := filepath.Join(dir, "dstmigrate")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Dir(here)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	must(bin, ".")
	migrated, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	if regexp.MustCompile(`(?m)^\s*go [a-z]`).Match(migrated) {
		t.Fatalf("a go statement survived:\n%s", migrated)
	}
	if got := must("go", "run", "."); got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
	if regexp.MustCompile(`goArg\d+ := (k|r)\b`).Match(migrated) {
		t.Errorf("a variable that never changes (k, or r, changed only through itself as a pointer) was bound:\n%s", migrated)
	}
}

// Verifies: [lstat-keeps-links]
// os.Lstat stays os.Lstat (reported): the real World's Stat follows
// symbolic links, so a migrated Lstat that became w.FS().Stat would
// describe a link's target in production, not the link. The program, on
// a dangling link, prints the same before and after migration.
// Fails if Lstat is rewritten to the World's Stat (the migrated program
// then fails its Lstat), or isn't reported.
func TestMigrateLstatKeepsLinks(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	kit := filepath.Join(filepath.Dir(here), "..", "..", "..")
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join(filepath.Dir(here), "testdata", "lstat", "p.go.txt"))
	if err != nil {
		t.Fatal(err)
	}
	run := func(name string, args ...string) string {
		t.Helper()
		cmd := exec.Command(name, args...)
		cmd.Dir = dir
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", name, args, err, out)
		}
		return string(out)
	}
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/lstat\n\ngo 1.25.0\n\nrequire github.com/prince776/go-dst-kit v0.0.0\n\nreplace github.com/prince776/go-dst-kit => "+kit+"\n"), 0o644)
	os.WriteFile(filepath.Join(dir, "p.go"), src, 0o644)
	want := run("go", "run", ".")
	if want != "lstat: symlink true\nstat: dangling\n" {
		t.Fatalf("the original program printed %q: the case doesn't test a dangling link", want)
	}
	bin := filepath.Join(dir, "dstmigrate")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Dir(here)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	warnings := run(bin, ".")
	migrated, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	if !strings.Contains(warnings, "os.Lstat has no World equivalent") {
		t.Errorf("os.Lstat wasn't reported:\n%s", warnings)
	}
	if got := run("go", "run", "."); got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
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
	for _, bad := range []string{`os\.(OpenFile|Open|Create|ReadFile|WriteFile|Rename|Remove|RemoveAll|Stat|MkdirAll|Mkdir|ReadDir|Link)\(`, `\*os\.File`, `ioutil\.WriteFile`, `\bf\.WriteString\(`} {
		if regexp.MustCompile(bad).MatchString(got) {
			t.Errorf("%q survived:\n%s", bad, got)
		}
	}
	for _, want := range []string{`s\.w\.FS\(\)\.Rename\(`, `dst\.Open\(s\.w\.FS\(\), `, `io\.WriteString\(f, "hello"\)`, `dst\.Link\(s\.w\.FS\(\), tmp, tmp\+"3"\)`, `func size\(f dst\.File\)`, `os\.IsNotExist\(err\)`,
		`\n// size takes a file: its type becomes dst\.File\.\nfunc size`} {
		if !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("want %q in:\n%s", want, got)
		}
	}
	if !strings.Contains(stderr, "os.Lstat has no World equivalent") || !strings.Contains(got, "os.Lstat(tmp)") {
		t.Errorf("os.Lstat wasn't kept and reported; stderr:\n%s", stderr)
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

// -map rewrites calls of a project's own clock wrapper, a function and a
// method, to functions taking the World first (importing their package),
// and -world finds the receiver of a generic type with two type
// parameters.
// Fails if clock.Now or Elapsed survives, the result doesn't build, the
// World isn't passed first (and the receiver next for the method), or
// the generic method gets the placeholder World.
func TestMigrateMap(t *testing.T) {
	_, here, _, _ := runtime.Caller(0)
	kit := filepath.Join(filepath.Dir(here), "..", "..", "..")
	dir := t.TempDir()
	for _, f := range []string{"p.go", "clock/clock.go", "dstclock/dstclock.go"} {
		src, err := os.ReadFile(filepath.Join(filepath.Dir(here), "testdata", "maps", f+".txt"))
		if err != nil {
			t.Fatal(err)
		}
		if f == "p.go" {
			src = []byte(strings.Replace(string(src), "import (", "import (\n\t\"github.com/prince776/go-dst-kit/dst\"", 1))
		}
		os.MkdirAll(filepath.Join(dir, filepath.Dir(f)), 0o755)
		os.WriteFile(filepath.Join(dir, f), src, 0o644)
	}
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example.com/migr\n\ngo 1.25.0\n\nrequire github.com/prince776/go-dst-kit v0.0.0\n\nreplace github.com/prince776/go-dst-kit => "+kit+"\n"), 0o644)
	bin := filepath.Join(dir, "dstmigrate")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Dir(here)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	cmd := exec.Command(bin, "-world", "Queue=$.w,shard=$.w",
		"-map", "example.com/migr/clock.Now=example.com/migr/dstclock.Now,(example.com/migr/clock.Mono).Elapsed=example.com/migr/dstclock.Elapsed", ".")
	cmd.Dir = dir
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("dstmigrate: %v\n%s", err, out)
	}
	vet := exec.Command("go", "vet", "./...")
	vet.Dir = dir
	got, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	if out, err := vet.CombinedOutput(); err != nil {
		t.Fatalf("vet: %v\n%s\n%s", err, out, got)
	}
	for _, want := range []string{`q\.last = dstclock\.Now\(q\.w\)`, `return dstclock\.Elapsed\(q\.w, q\.last\)`, `s\.ch = dst\.MakeChan\[V\]\(s\.w, 1\)`, `"example\.com/migr/dstclock"`} {
		if !regexp.MustCompile(want).Match(got) {
			t.Errorf("want %q in:\n%s", want, got)
		}
	}
	if regexp.MustCompile(`clock\.Now\(\)|\.Elapsed\(\)|__w`).Match(got) {
		t.Errorf("a mapped call or the placeholder World survived:\n%s", got)
	}
}
