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
// without moving the comment after it; math/rand and math/rand/v2 calls
// (N, NormFloat64) and crypto/rand.Reader become the World's by name,
// and runtime.Gosched becomes w.Yield. Fails if any survives, the result
// doesn't build, the nil argument is kept or moves the comment, or a
// rand call is reported or renamed.
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
	if strings.Contains(warnings, "rand.") {
		t.Errorf("a rand function was reported: each has its World equivalent:\n%s", warnings)
	}
	if !regexp.MustCompile(`p\.wd\.Rand\(\)\.NormFloat64\(\)`).Match(got) || !regexp.MustCompile(`dst\.N\(p\.wd\.Rand\(\), d\)`).Match(got) || !regexp.MustCompile(`p\.wd\.MathRand\(\)\.Intn\(3\)`).Match(got) {
		t.Errorf("rand calls not rewritten to the World's by name:\n%s", got)
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
	want, got, migrated, warnings := beforeAndAfter(t, "lstat")
	if want != "lstat: symlink true\nstat: dangling\n" {
		t.Fatalf("the original program printed %q: the case doesn't test a dangling link", want)
	}
	if !strings.Contains(warnings, "os.Lstat has no World equivalent") {
		t.Errorf("os.Lstat wasn't reported:\n%s", warnings)
	}
	if got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
}

// Verifies: [inexact-is-kept]
// The std APIs with an exact World counterpart migrate to it, and the
// program prints the same before and after: context's Cause variants
// (the cause reaches context.Cause; the select on Done is migrated and
// sees it end), context.AfterFunc, time.Tick, sync.OnceValue,
// crypto/rand.Text, math/rand (v1) and v2 calls, and rand.N on an
// unsigned type beyond MaxInt64.
// Fails if any of them is rewritten inexactly (the Cause variants used
// to lose the cause, and WithCancelCause its parent: the result didn't
// build; rand.N on an unsigned type used to become Int64N, which panics
// beyond MaxInt64) or not at all.
func TestMigrateExact(t *testing.T) {
	want, got, migrated, warnings := beforeAndAfter(t, "cause")
	if !strings.Contains(want, "done: boom") || !strings.Contains(want, "unsigned N in range: true") {
		t.Fatalf("the original program printed %q", want)
	}
	for _, re := range []string{`dst\.WithCancelCause\(__w, context\.Background\(\)\)`, `dst\.WithTimeoutCause\(__w, ctx, 0, errBoom\)`, `dst\.Select\(`,
		`dst\.N\(__w\.Rand\(\), big\+5\)`, `dst\.AfterDone\(`, `__w\.Tick\(`, `dst\.OnceValue\(`, `dst\.CryptoText\(__w\)`, `__w\.MathRand\(\)\.Intn\(`, `__w\.Rand\(\)\.Uint64N\(`} {
		if !regexp.MustCompile(re).MatchString(migrated) {
			t.Errorf("want %s in:\n%s", re, migrated)
		}
	}
	if strings.Contains(warnings, "no World equivalent") {
		t.Errorf("a call with an exact equivalent was reported:\n%s", warnings)
	}
	if got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
}

// Verifies: [inexact-is-kept]
// io.Pipe, net.Pipe, net.Listen, Dial, DialTimeout, http.Serve, http.Get
// and http.DefaultClient migrate to the World's (std's objects in the
// real World: a setting on DefaultClient still applies), and the program
// prints the same before and after; *io.PipeReader becomes dst's.
// Fails if any is left as is or rewritten inexactly (DefaultClient as a
// new client loses its setting).
func TestMigrateNetIO(t *testing.T) {
	want, got, migrated, warnings := beforeAndAfter(t, "netio")
	if !strings.Contains(want, "http hello 200 5s") {
		t.Fatalf("the original program printed %q", want)
	}
	for _, re := range []string{`dst\.Pipe\(__w\)`, `dst\.NetPipe\(__w\)`, `__w\.Net\(\)\.Listen\(`, `__w\.Net\(\)\.Dial\(`, `dst\.DialTimeout\(__w, `,
		`dsthttp\.Serve\(goArg\d+, hl, mux\)`, `dsthttp\.DefaultClient\(__w\)\.Get\(`, `dsthttp\.DefaultClient\(__w\)\.Timeout = `, `func pipeRead\(r \*dst\.PipeReader\)`} {
		if !regexp.MustCompile(re).MatchString(migrated) {
			t.Errorf("want %s in:\n%s", re, migrated)
		}
	}
	if regexp.MustCompile(`\b(io|net|http)\.(Pipe|Listen|Dial|DialTimeout|Serve|Get)\(`).MatchString(migrated) {
		t.Errorf("a std call survived:\n%s", migrated)
	}
	if strings.Contains(warnings, "no World equivalent") {
		t.Errorf("reported:\n%s", warnings)
	}
	if got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
}

// Verifies: [inexact-is-kept]
// The rest of os's filesystem API (Chmod, Chtimes, Truncate, MkdirTemp,
// CreateTemp, DirFS, ioutil's TempDir and TempFile), filepath.Walk,
// WalkDir and Glob, and *os.File's Chmod, Readdir and Readdirnames
// migrate to dst's helpers, and the program prints the same before and
// after (including a walk past a symbolic link, which filepath doesn't
// follow).
// Fails if any is left or reported, or behaves differently once migrated.
func TestMigrateFSMore(t *testing.T) {
	want, got, migrated, warnings := beforeAndAfter(t, "fsmore")
	if !strings.Contains(want, "walk: d d/x.txt link") {
		t.Fatalf("the original program printed %q", want)
	}
	if regexp.MustCompile(`\b(os|filepath)\.(Chmod|Chtimes|Truncate|MkdirTemp|CreateTemp|DirFS|Walk|WalkDir|Glob)\(|\bioutil\.|\b(d|f)\.(Chmod|Readdir|Readdirnames)\(`).MatchString(migrated) {
		t.Errorf("a std call survived:\n%s", migrated)
	}
	for _, re := range []string{`dst\.WalkDir\(__w\.FS\(\), `, `dst\.MkdirTemp\(__w\.FS\(\), "", `, `dst\.CreateTemp\(__w\.FS\(\), `, `dst\.FileChmod\(f, `, `dst\.Readdirnames\(d, -1\)`, `dst\.DirFS\(__w\.FS\(\), `} {
		if !regexp.MustCompile(re).MatchString(migrated) {
			t.Errorf("want %s in:\n%s", re, migrated)
		}
	}
	for _, l := range strings.Split(warnings, "\n") {
		// os.Symlink sets the program up (symbolic links are still to come).
		if (strings.Contains(l, "no World equivalent") || strings.Contains(l, "isn't part of dst.File")) && !strings.Contains(l, "os.Symlink") {
			t.Errorf("reported: %s", l)
		}
	}
	if got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
}

// Verifies: [inexact-is-kept]
// grpc.NewServer and grpc.NewClient migrate to dstgrpc's exact
// constructors, grpc's interceptor options to dstgrpc's carrying ones,
// *grpc.ClientConn (but in an interceptor's signature, where grpc passes
// its own) and *grpc.Server to dstgrpc's, and the program prints
// the same before and after: a client without credentials still fails
// with grpc's error, interceptors run in grpc's order. Options the tool
// can't see into (a variable, a spread slice) and grpc options that
// don't apply in simulation are reported, naming the call site, and kept.
// Fails if a grpc constructor or interceptor option survives, the
// output changes, or an opaque option goes unreported.
func TestMigrateGRPC(t *testing.T) {
	want, got, migrated, warnings := beforeAndAfter(t, "grpc")
	if !strings.Contains(want, "no transport security set") || !strings.Contains(want, "order: cu cc1 su sc1") {
		t.Fatalf("the original program printed %q", want)
	}
	if regexp.MustCompile(`\bgrpc\.(NewServer|NewClient|UnaryInterceptor|ChainUnaryInterceptor|WithUnaryInterceptor|WithChainUnaryInterceptor)\(|\*grpc\.Server\b|call\(c \*grpc\.ClientConn`).MatchString(migrated) {
		t.Errorf("a grpc constructor, option or type survived:\n%s", migrated)
	}
	for _, re := range []string{`dstgrpc\.NewServerExact\(__w, dstgrpc\.UnaryInterceptorOption\(`, `dstgrpc\.NewClientExact\(__w, `, `func call\(c dstgrpc\.ClientConn\)`, `func stop\(s \*dstgrpc\.Server\)`,
		`cc \*grpc\.ClientConn, inv grpc\.UnaryInvoker`} { // an interceptor's connection is grpc's
		if !regexp.MustCompile(re).MatchString(migrated) {
			t.Errorf("want %s in:\n%s", re, migrated)
		}
	}
	for _, w := range []string{"p.go:", "can't see into these options of grpc.NewClient", "can't see into this option of grpc.NewServer", "grpc.MaxRecvMsgSize (an option of grpc.NewServer) has no effect in a simulated World"} {
		if !strings.Contains(warnings, w) {
			t.Errorf("want %q reported:\n%s", w, warnings)
		}
	}
	if got != want {
		t.Errorf("the migrated program prints\n%s\nthe original\n%s\nmigrated source:\n%s", got, want, migrated)
	}
}

// beforeAndAfter runs testdata/<name>/p.go.txt as a program on the real
// World, migrates it (no -world: it defines __w), runs it again, and
// returns both outputs, the migrated source and the tool's warnings.
func beforeAndAfter(t *testing.T, name string) (want, got, migrated, warnings string) {
	t.Helper()
	_, here, _, _ := runtime.Caller(0)
	kit := filepath.Join(filepath.Dir(here), "..", "..", "..")
	dir := t.TempDir()
	src, err := os.ReadFile(filepath.Join(filepath.Dir(here), "testdata", name, "p.go.txt"))
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
	gomod := "module example.com/" + name + "\n\ngo 1.25.0\n\nrequire github.com/prince776/go-dst-kit v0.0.0\n\nreplace github.com/prince776/go-dst-kit => " + kit + "\n"
	if strings.Contains(string(src), `"google.golang.org/grpc"`) {
		// dstgrpc is a module of its own, with grpc's requirements.
		g, err := os.ReadFile(filepath.Join(kit, "dstgrpc", "go.mod"))
		if err != nil {
			t.Fatal(err)
		}
		reqs := string(g)[strings.Index(string(g), "require ("):strings.Index(string(g), "replace")]
		gomod += "\nrequire github.com/prince776/go-dst-kit/dstgrpc v0.0.0\n\nreplace github.com/prince776/go-dst-kit/dstgrpc => " + filepath.Join(kit, "dstgrpc") + "\n\n" + reqs
		sum, _ := os.ReadFile(filepath.Join(kit, "dstgrpc", "go.sum"))
		os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0o644)
	}
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644)
	os.WriteFile(filepath.Join(dir, "p.go"), src, 0o644)
	tidy := func() {
		if !strings.Contains(gomod, "dstgrpc") {
			return
		}
		cmd := exec.Command("go", "mod", "tidy")
		cmd.Dir = dir
		cmd.Env = append(os.Environ(), "GOPROXY=off", "GOFLAGS=-mod=mod")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Skipf("go mod tidy (from the module cache only): %v\n%s", err, out)
		}
	}
	tidy()
	want = run("go", "run", ".")
	bin := filepath.Join(dir, "dstmigrate")
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = filepath.Dir(here)
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	warnings = run(bin, ".")
	m, _ := os.ReadFile(filepath.Join(dir, "p.go"))
	migrated = string(m)
	os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0o644) // what the migration needs
	tidy()
	cmd := exec.Command("go", "run", ".")
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("the migrated program failed: %v\n%s\nmigrated source:\n%s", err, out, migrated)
	}
	return want, string(out), migrated, warnings
}

// File I/O through package os (and io/ioutil) migrates to the World's FS:
// the result compiles and vets, no os filesystem call or *os.File is
// left, WriteString becomes io.WriteString, and a call with no World
// equivalent (os.Chown) is reported, not silently kept; comments stay
// where they were.
// Fails if any os filesystem call or *os.File survives, the result
// doesn't build, os.Chown goes unreported, or a rewritten call moves the
// comment after it.
func TestMigrateFiles(t *testing.T) {
	got, stderr := migrateSample(t, "files", "Store=$.w")
	for _, bad := range []string{`os\.(OpenFile|Open|Create|ReadFile|WriteFile|Rename|Remove|RemoveAll|Stat|MkdirAll|Mkdir|ReadDir|Link|Chmod)\(`, `\*os\.File`, `ioutil\.WriteFile`, `\bf\.WriteString\(`} {
		if regexp.MustCompile(bad).MatchString(got) {
			t.Errorf("%q survived:\n%s", bad, got)
		}
	}
	for _, want := range []string{`s\.w\.FS\(\)\.Rename\(`, `dst\.Open\(s\.w\.FS\(\), `, `io\.WriteString\(f, "hello"\)`, `dst\.Link\(s\.w\.FS\(\), tmp, tmp\+"3"\)`, `func size\(f dst\.File\)`, `os\.IsNotExist\(err\)`, `dst\.Chmod\(s\.w\.FS\(\), `,
		`\n// size takes a file: its type becomes dst\.File\.\nfunc size`} {
		if !regexp.MustCompile(want).MatchString(got) {
			t.Errorf("want %q in:\n%s", want, got)
		}
	}
	if !strings.Contains(stderr, "os.Lstat has no World equivalent") || !strings.Contains(got, "os.Lstat(tmp)") {
		t.Errorf("os.Lstat wasn't kept and reported; stderr:\n%s", stderr)
	}
	if !strings.Contains(stderr, "os.Chown has no World equivalent") {
		t.Errorf("os.Chown wasn't reported; stderr:\n%s", stderr)
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
// os.Chown is reported.
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
