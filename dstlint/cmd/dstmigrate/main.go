// Command dstmigrate rewrites a package to run in a dst.World: the
// mechanical part of adopting go-dst-kit.
//
// It loads the package with type information and rewrites, in place:
//
//   - channel types to dst.Chan[T], make(chan T, n) to dst.MakeChan[T](w, n);
//   - sends to c.Send(v), receives to c.Recv() (or dstRecv(c) where only
//     the value is used), range over a channel to range c.All(), close,
//     len and cap of channels to their methods, and a channel compared
//     with, set to, passed as or put in a literal as nil to the zero
//     dst.Chan;
//   - select statements to dst.Select, keeping their control flow: each
//     case's callback records that it fired (and what it received), and a
//     switch after the select runs the original case bodies, so break,
//     continue and return mean what they did;
//   - time.Now, Since, Until, Sleep, After, AfterFunc, NewTimer,
//     NewTicker and Tick to the World's methods, *time.Timer and
//     *time.Ticker to *dst.Timer and *dst.Ticker;
//   - context.WithCancel, WithTimeout, WithDeadline and their Cause
//     variants to dst's, context.AfterFunc to dst.AfterDone;
//   - sync.Mutex, RWMutex, WaitGroup, Once, Cond and Pool to their dst
//     types, sync.NewCond, OnceFunc, OnceValue and OnceValues to dst's;
//   - go statements to w.Go, evaluating the call's arguments first as go
//     does, and wg.Go(f) on a sync.WaitGroup to wg.Go(w, f);
//   - runtime.Gosched to w.Yield (a spin loop must let others run);
//   - math/rand/v2's functions to the World's Rand by the same names
//     (rand.N to dst.N), math/rand's (v1) to its MathRand by the same
//     names (Seed too), and crypto/rand.Read, Reader and Text to the
//     World's;
//   - io.Pipe and net.Pipe to dst.Pipe and dst.NetPipe (*io.PipeReader
//     and *io.PipeWriter to dst's), net.Dial, Listen and ListenPacket to
//     w.Net()'s, net.DialTimeout to dst.DialTimeout;
//   - http.Get, Post, Head and PostForm to dsthttp.DefaultClient(w)'s,
//     http.DefaultClient and DefaultTransport to dsthttp's, http.Serve and
//     ListenAndServe to dsthttp's (each the std object in the real World);
//   - grpc.NewServer and NewClient to dstgrpc.NewServerExact and
//     NewClientExact (grpc's own calls in the real World), grpc's
//     interceptor options to dstgrpc's carrying ones, *grpc.ClientConn
//     and *grpc.Server to dstgrpc's (but in interceptor signatures);
//     options it can't see into are reported, as are grpc.Dial and
//     DialContext;
//   - file I/O to the World's FS: os.OpenFile, Mkdir, MkdirAll, Remove,
//     RemoveAll, Rename, Stat, ReadDir to w.FS()'s methods;
//     os.Open, Create, ReadFile, WriteFile, Link, Chmod, Chtimes,
//     Truncate, MkdirTemp, CreateTemp, DirFS (and ioutil's ReadFile,
//     WriteFile, TempDir, TempFile) and filepath.Walk, WalkDir and Glob to
//     dst's helpers, each os's or filepath's own function in the
//     production World; *os.File to dst.File, f.WriteString to
//     io.WriteString, f.Chmod, Readdir and Readdirnames to dst's helpers.
//     Names pass through unchanged: the production real World's FS is
//     the os package, so they mean what they did; a simulated disk (and a
//     real World made with a Dir) is rooted, so code that runs simulated
//     uses names relative to it. Calls with no World equivalent
//     (os.Chown, os.Symlink, (*os.File).Fd, ...) are reported.
//     -real-fs names files whose file I/O stays on the real filesystem
//     on purpose (tests' temporary directories, operator tools): base
//     name patterns, as path.Match takes them.
//
// -map rewrites calls of other functions and methods (a project's own
// clock wrappers, say) to functions that take the World first:
// -map 'example.com/clock.Now=example.com/app/dstclock.Now' turns
// clock.Now() into dstclock.Now(w), and
// '(example.com/clock.Mono).Elapsed=example.com/app/dstclock.Elapsed'
// turns m.Elapsed() into dstclock.Elapsed(w, m).
//
// The World expression is chosen by the enclosing method's receiver:
// -world 'Raft=$.w,NetworkTransport=$.w' uses r.w in methods of Raft
// whose receiver is named r. Elsewhere it writes __w, which won't
// compile: each such place needs a World passed in by hand. Review the
// result: the rewrite is mechanical, and code relying on raw channel
// identity, reflection over channels, or channels from other packages
// needs a person.
//
// -gosched names functions whose runtime.Gosched stays w.Gosched (a turn
// for others in a loop that makes its own progress) instead of w.Yield.
// -test-world may name its package by import path
// ('example.com/app/dstworld.Real()'): test files using it import it.
//
// -world-param names functions given the World as a new first
// parameter, w, every call passing the World at hand; -world-field names
// config types (Config.World) whose functions take the World from that
// field, the real World when nil. A literal of a -world type that leaves
// its World field unset gets the World at hand.
//
// Usage: dstmigrate [-world MAP] [-world-param FUNCS] [-world-field
// TYPE.FIELD] [-map MAP] [-tests] [-test-world EXPR] [-real-fs PATTERNS]
// [-gosched FUNCS] [-n] packages...
//
// Verified:
//   - [lstat-keeps-links] os.Lstat is kept and reported, not rewritten to
//     the World's Stat: the real World's Stat follows symbolic links, so
//     the rewrite would change what migrated code sees in production.
//   - [inexact-is-kept] A call is rewritten only to its exact World
//     equivalent, so migrated code behaves in production as it did; a
//     call without one (os.Chown, (*os.File).Fd, ...) is kept and
//     reported.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"maps"
	"os"
	"path"
	"path/filepath"
	"slices"
	"sort"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

const (
	dstPath       = "github.com/prince776/go-dst-kit/dst"
	dsthttpPath   = "github.com/prince776/go-dst-kit/dsthttp"
	dstgrpcPath   = "github.com/prince776/go-dst-kit/dstgrpc"
	realworldPath = "github.com/prince776/go-dst-kit/realworld"
	grpcPath      = "google.golang.org/grpc"
)

// grpcInterceptors are grpc's interceptor options; each becomes
// dstgrpc's carrying one (the same grpc option in the real World, its
// interceptor carried into a simulated one).
var grpcInterceptors = map[string]bool{"UnaryInterceptor": true, "StreamInterceptor": true, "ChainUnaryInterceptor": true,
	"ChainStreamInterceptor": true, "WithUnaryInterceptor": true, "WithStreamInterceptor": true,
	"WithChainUnaryInterceptor": true, "WithChainStreamInterceptor": true}

// grpcChecked are grpc's credential options: a simulated World checks
// them as grpc does (NewClientExact refuses a client without any).
var grpcChecked = map[string]bool{"WithTransportCredentials": true, "WithCredentialsBundle": true, "WithInsecure": true, "Creds": true}

// grpcCall rewrites grpc.NewServer and grpc.NewClient to dstgrpc's exact
// constructors and grpc's interceptor options to dstgrpc's carrying
// ones, and reports what a simulated World won't see.
func (m *migrator) grpcCall(n *ast.CallExpr, s *ast.SelectorExpr, pos token.Pos) {
	name := s.Sel.Name
	switch {
	case name == "NewServer" || name == "NewClient":
		opts := n.Args
		if name == "NewClient" && len(opts) > 0 {
			opts = opts[1:]
		}
		m.grpcOptions(n, name, opts)
		n.Fun = sel(ast.NewIdent("dstgrpc"), name+"Exact")
		n.Args = append([]ast.Expr{m.w()}, n.Args...)
	case grpcInterceptors[name]:
		n.Fun = sel(ast.NewIdent("dstgrpc"), name+"Option")
	case name == "Dial" || name == "DialContext":
		m.warn(n, "grpc."+name+" has no World equivalent (it connects at once, by the passthrough resolver); grpc.NewClient has (dstgrpc.NewClientExact)")
		return
	default:
		return
	}
	m.need(dstgrpcPath)
	setPos(n, pos)
	m.changes++
}

// grpcOptions reports the options of a grpc.NewServer or NewClient call
// that a simulated World won't apply: grpc options other than
// interceptors and credentials (its transport isn't grpc's), and any
// option this tool can't see into (a variable, another package's
// function, a spread slice), whose interceptors would not run there.
func (m *migrator) grpcOptions(n *ast.CallExpr, fn string, opts []ast.Expr) {
	for i, o := range opts {
		if c, ok := o.(*ast.CallExpr); ok {
			if s, ok := c.Fun.(*ast.SelectorExpr); ok {
				if id, ok := s.X.(*ast.Ident); ok {
					if m.info.Uses[id] == nil && id.Name == "dstgrpc" {
						continue // an interceptor option this migration just rewrote
					}
					if pn, ok := m.info.Uses[id].(*types.PkgName); ok {
						switch pn.Imported().Path() {
						case grpcPath:
							if grpcInterceptors[s.Sel.Name] || grpcChecked[s.Sel.Name] {
								continue
							}
							m.warn(o, "grpc."+s.Sel.Name+" (an option of grpc."+fn+") has no effect in a simulated World, whose transport isn't grpc's")
							continue
						case dstgrpcPath:
							continue
						}
					}
				}
			}
		}
		what := "this option"
		if n.Ellipsis.IsValid() && i == len(opts)-1 {
			what = "these options"
		}
		m.warn(o, "dstmigrate can't see into "+what+" of grpc."+fn+": an interceptor in them would not run in a simulated World (make it with dstgrpc.UnaryInterceptorOption and its kin)")
	}
}

// markInterceptorConns records in keepGRPC the *grpc.ClientConn types in
// d's interceptor signatures.
func (m *migrator) markInterceptorConns(d ast.Node) {
	ast.Inspect(d, func(n ast.Node) bool {
		ft, ok := n.(*ast.FuncType)
		if !ok || ft.Params == nil {
			return true
		}
		interceptor := false
		for _, f := range ft.Params.List {
			if named, ok := m.info.TypeOf(f.Type).(*types.Named); ok && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == grpcPath &&
				(named.Obj().Name() == "UnaryInvoker" || named.Obj().Name() == "Streamer") {
				interceptor = true
			}
		}
		if interceptor {
			for _, f := range ft.Params.List {
				if m.keepGRPC == nil {
					m.keepGRPC = map[ast.Node]bool{}
				}
				m.keepGRPC[f.Type] = true
			}
		}
		return true
	})
}

// grpcClientMethods are the *grpc.ClientConn methods dstgrpc.ClientConn
// has; grpcServerMethods the *grpc.Server ones *dstgrpc.Server has.
var (
	grpcClientMethods = map[string]bool{"Invoke": true, "NewStream": true, "Close": true}
	grpcServerMethods = map[string]bool{"RegisterService": true, "Serve": true, "Stop": true, "GracefulStop": true}
)

// grpcMethod reports a method call on a *grpc.ClientConn or *grpc.Server
// (which the migration makes dstgrpc's) that dstgrpc's lacks.
func (m *migrator) grpcMethod(n *ast.CallExpr, s *ast.SelectorExpr) {
	p, ok := m.info.TypeOf(s.X).(*types.Pointer)
	if !ok {
		return
	}
	named, ok := p.Elem().(*types.Named)
	if !ok || named.Obj().Pkg() == nil || named.Obj().Pkg().Path() != grpcPath {
		return
	}
	switch named.Obj().Name() {
	case "ClientConn":
		if !grpcClientMethods[s.Sel.Name] {
			m.warn(n, "(*grpc.ClientConn)."+s.Sel.Name+" isn't part of dstgrpc.ClientConn")
		}
	case "Server":
		if !grpcServerMethods[s.Sel.Name] {
			m.warn(n, "(*grpc.Server)."+s.Sel.Name+" isn't part of *dstgrpc.Server")
		}
	}
}

// need records that the file's rewrites use package path.
func (m *migrator) need(path string) {
	if m.imports == nil {
		m.imports = map[string]bool{}
	}
	m.imports[path] = true
}

func main() {
	worldFlag := flag.String("world", "", "receiver type -> World expression, $ for the receiver: 'Raft=$.w,Pool=$.world'")
	dryRun := flag.Bool("n", false, "print the files instead of writing them")
	tests := flag.Bool("tests", false, "also rewrite the packages' tests (from the package's test build, in the same pass)")
	testWorld := flag.String("test-world", "__w", "the World expression in test files where no -world mapping applies; a package by import path ('example.com/app/dstworld.Real()') is imported where used")
	mapFlag := flag.String("map", "", "calls rewritten to a function taking the World first: 'importpath.Func=importpath.Func,(importpath.Type).Method=importpath.Func'")
	realFS := flag.String("real-fs", "", "files whose os file I/O is left as is, by base name pattern: 'peersjson.go,*_test.go'")
	worldParamFlag := flag.String("world-param", "", "functions given the World as a new first parameter, w, with every call passing the World at hand: 'Func,Type.Method'")
	worldFieldFlag := flag.String("world-field", "", "config types whose field holds the World: a function given one takes its World from the field (the real World when nil); the field is added if missing: 'Config.World'")
	goschedFlag := flag.String("gosched", "", "functions whose runtime.Gosched is a turn for others in a loop that makes its own progress, kept as w.Gosched (elsewhere it becomes w.Yield): 'Type.Method,Func'")
	flag.Parse()
	gosched := map[string]bool{}
	for _, name := range strings.Split(*goschedFlag, ",") {
		if name = strings.TrimSpace(name); name != "" {
			gosched[name] = false // set once a function of that name is met
		}
	}
	threading := parseWorldThreading(*worldParamFlag, *worldFieldFlag)
	worlds := map[string]string{}
	for _, kv := range strings.Split(*worldFlag, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
			worlds[k] = v
		}
	}
	callMaps, err := parseMaps(*mapFlag)
	if err != nil {
		fatal(err)
	}
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo, Tests: *tests}
	pkgs, err := packages.Load(cfg, flag.Args()...)
	if err != nil {
		fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		os.Exit(1)
	}
	seen := map[string]bool{}
	for _, pkg := range pkgs {
		if *tests && skipForTests(pkg, pkgs) {
			continue
		}
		tw, twPath := worldImport(*testWorld)
		m := &migrator{testWorld: tw, testWorldPath: twPath, pkg: pkg.Types, fset: pkg.Fset, info: pkg.TypesInfo, worlds: worlds, maps: callMaps, gosched: gosched, threading: &threading}
		for i, f := range pkg.Syntax {
			name := pkg.CompiledGoFiles[i]
			if strings.HasSuffix(name, ".go") == false || seen[name] {
				continue // generated test main, or a file already done
			}
			seen[name] = true
			m.inTest = strings.HasSuffix(name, "_test.go")
			m.realFS = matchAny(*realFS, filepath.Base(name))
			m.plans = nil
			if !m.file(f) {
				continue
			}
			var buf bytes.Buffer
			if err := format.Node(&buf, pkg.Fset, f); err != nil {
				var raw bytes.Buffer
				printer.Fprint(&raw, pkg.Fset, f)
				fatal(fmt.Errorf("%s: %w\n%s", name, err, raw.String()))
			}
			// Plans were made after each select's children (inner first);
			// the text pass counts in source order (outer first).
			sort.Slice(m.plans, func(i, j int) bool { return m.plans[i].pos < m.plans[j].pos })
			spliced, err := spliceSelects(name, buf.Bytes(), m.plans)
			if err != nil {
				fatal(fmt.Errorf("%s: %w", name, err))
			}
			spliced = m.spliceWorld(spliced)
			m.fills = nil
			if formatted, err := format.Source(spliced); err == nil {
				spliced = formatted
			}
			out, err := imports.Process(name, spliced, nil)
			if err != nil {
				out = buf.Bytes() // left for the compiler to explain
			}
			if *dryRun {
				fmt.Printf("// ---- %s\n%s", name, out)
				continue
			}
			if err := os.WriteFile(name, out, 0o644); err != nil {
				fatal(err)
			}
			fmt.Fprintf(os.Stderr, "rewrote %s (%d changes)\n", name, m.changes)
		}
		if m.needRecv && !*dryRun {
			// The helper for receives used as a value, once per package.
			dir := filepath.Dir(pkg.CompiledGoFiles[0])
			src := "package " + pkg.Types.Name() + "\n\nimport \"" + dstPath + "\"\n\n" +
				"// dstRecv is <-c where only the value is used (written by dstmigrate).\n" +
				"func dstRecv[T any](c dst.Chan[T]) T { v, _ := c.Recv(); return v }\n"
			if err := os.WriteFile(filepath.Join(dir, "dst_recv.go"), []byte(src), 0o644); err != nil {
				fatal(err)
			}
		}
	}
	for _, name := range slices.Sorted(maps.Keys(gosched)) {
		if !gosched[name] {
			fmt.Fprintf(os.Stderr, "dstmigrate: -gosched names %s, which no migrated package has\n", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(threading.params)) {
		if !threading.params[name] {
			fmt.Fprintf(os.Stderr, "dstmigrate: -world-param names %s, which no migrated package declares\n", name)
		}
	}
	for _, name := range slices.Sorted(maps.Keys(threading.fields)) {
		if !threading.met[name] {
			fmt.Fprintf(os.Stderr, "dstmigrate: -world-field names %s, which no migrated package declares\n", name)
		}
	}
}

type migrator struct {
	// keepGRPC are the *grpc.ClientConn types in interceptor signatures
	// (a parameter list with a grpc.UnaryInvoker or grpc.Streamer): grpc
	// passes its own connection there, so they stay grpc's.
	keepGRPC  map[ast.Node]bool
	warned    map[string]bool
	testWorld string
	// gosched names the functions (Type.Method, Func) whose
	// runtime.Gosched stays w.Gosched; a value is set once one is met.
	gosched map[string]bool
	// threading is -world-param and -world-field.
	threading *worldThreading
	// funcName is the function being rewritten, as gosched names it.
	funcName string
	// testWorldPath is the package testWorld's expression uses, imported
	// where it is used ("" for none).
	testWorldPath string
	inTest        bool
	realFS        bool // the file's os file I/O is left as is (-real-fs)
	pkg           *types.Package
	fset          *token.FileSet
	info          *types.Info
	worlds        map[string]string
	maps          map[string]mapping // -map: calls rewritten to a function taking the World
	imports       map[string]bool    // packages the file's rewrites need
	changes       int
	needRecv      bool
	world         string   // the World expression in the current function
	wUses         int      // how often a World expression was written
	fills         []string // the file's World fields fillWorld leaves to spliceWorld
	// worldArgs are the World arguments worldParamCall passed: a go
	// statement doesn't copy them first (a World doesn't change).
	worldArgs map[ast.Expr]bool
	plans     []selectPlan // the file's selects, in source order, for the text pass
	selN      int
	mutated   map[types.Object]bool // the file's variables that change (mutatedIn)
}

// isChan reports whether e is a channel this migration converts: one of
// the package's own, not one another package hands out (ctx.Done(), a
// library's field), which stays a raw channel.
func (m *migrator) isChan(e ast.Expr) bool {
	t := m.info.TypeOf(e)
	if t == nil {
		return false
	}
	if _, ok := t.Underlying().(*types.Chan); !ok {
		return false
	}
	if m.foreign(e) {
		if !m.inTest && m.ctxDone(e) == "" {
			m.warn(e, "a channel from another package stays raw; waiting on it can't be simulated")
		}
		return false
	}
	return true
}

// foreign reports whether e is a channel from another package: a call to
// a function or method declared there, or a field of a type declared there.
func (m *migrator) foreign(e ast.Expr) bool {
	var obj types.Object
	switch x := ast.Unparen(e).(type) {
	case *ast.CallExpr:
		switch f := ast.Unparen(x.Fun).(type) {
		case *ast.Ident:
			obj = m.info.Uses[f]
		case *ast.SelectorExpr:
			obj = m.info.Uses[f.Sel]
		}
	case *ast.SelectorExpr:
		obj = m.info.Uses[x.Sel]
		// A timer's or ticker's C: the migration makes it a dst.Timer's.
		if v, ok := obj.(*types.Var); ok && v.IsField() && v.Name() == "C" && v.Pkg() != nil && v.Pkg().Path() == "time" {
			return false
		}
	}
	if m.ctxDone(e) != "" {
		return true // a context's Done: select cases use dst.OnDone; elsewhere it stays raw
	}
	return obj != nil && obj.Pkg() != nil && obj.Pkg() != m.pkg
}

func (m *migrator) warn(n ast.Node, msg string) {
	p := m.fset.Position(n.Pos())
	key := p.String() + msg
	if m.warned == nil {
		m.warned = map[string]bool{}
	}
	if !m.warned[key] {
		m.warned[key] = true
		fmt.Fprintf(os.Stderr, "%s: %s\n", p, msg)
	}
}

func (m *migrator) chanElem(e ast.Expr) string {
	return m.typeString(m.info.TypeOf(e).Underlying().(*types.Chan).Elem())
}

// typeString prints t as the migrated code spells it: a channel type
// inside it (a channel of channels, a slice of them) is a dst.Chan, as the
// rewritten declarations say.
func (m *migrator) typeString(t types.Type) string {
	switch t := t.(type) {
	case *types.Chan:
		return "dst.Chan[" + m.typeString(t.Elem()) + "]"
	case *types.Pointer:
		return "*" + m.typeString(t.Elem())
	case *types.Slice:
		return "[]" + m.typeString(t.Elem())
	case *types.Array:
		return fmt.Sprintf("[%d]%s", t.Len(), m.typeString(t.Elem()))
	case *types.Map:
		return "map[" + m.typeString(t.Key()) + "]" + m.typeString(t.Elem())
	}
	return types.TypeString(t, m.qualifier)
}

// qualifier names types as code in the migrated package would: its own
// unqualified, others by package name.
func (m *migrator) qualifier(p *types.Package) string {
	if p == m.pkg {
		return ""
	}
	return p.Name()
}

func expr(src string) ast.Expr {
	e, err := parseExpr(src)
	if err != nil {
		panic(fmt.Sprintf("dstmigrate: bad generated expression %q: %v", src, err))
	}
	return e
}

func (m *migrator) w() ast.Expr {
	m.wUses++
	if m.testWorldPath != "" && m.world == m.testWorld {
		m.need(m.testWorldPath)
	}
	return expr(m.world)
}

// worldImport splits a World expression naming its package by import
// path ("example.com/app/dstworld.Real()") into the expression as code
// writes it ("dstworld.Real()") and the path to import. go-dst-kit's
// realworld is known by its name alone ("realworld.New(...)").
func worldImport(e string) (expr, path string) {
	if i := strings.LastIndex(e[:max(strings.IndexAny(e, "(["), 0)], "/"); strings.IndexAny(e, "([") > 0 && i > 0 {
		rest := e[i+1:]
		if dot := strings.Index(rest, "."); dot > 0 {
			return rest, e[:i+1] + rest[:dot]
		}
	}
	if strings.HasPrefix(e, "realworld.") {
		return e, realworldPath
	}
	return e, ""
}

// file rewrites f; it reports whether anything changed.
func (m *migrator) file(f *ast.File) bool {
	before := m.changes
	m.mutated = m.mutatedIn(f)
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		m.world = "__w"
		if m.inTest {
			m.world = m.testWorld
		}
		m.funcName = ""
		if ok {
			m.funcName = fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) == 1 {
				m.funcName = recvType(fd.Recv.List[0].Type) + "." + fd.Name.Name
			}
			if _, listed := m.gosched[m.funcName]; listed {
				m.gosched[m.funcName] = true
			}
		}
		mapped := false
		if ok && fd.Recv != nil && len(fd.Recv.List) == 1 {
			tname := recvType(fd.Recv.List[0].Type)
			if tmpl, ok := m.worlds[tname]; ok && len(fd.Recv.List[0].Names) == 1 {
				m.world = strings.ReplaceAll(tmpl, "$", fd.Recv.List[0].Names[0].Name)
				mapped = true
			}
		}
		fallback := ""
		switch {
		case !ok || mapped:
		case m.worldParam(fd):
			m.world = "w"
		default:
			if from := m.configParam(fd); from != "" && !m.declaresW(fd) {
				m.world, fallback = "w", from
			}
		}
		uses := m.wUses
		m.decl(d)
		if fallback != "" && m.wUses > uses {
			m.worldFallback(fd, fallback)
		}
	}
	m.addWorldFields(f)
	if m.changes > before {
		astutil.AddImport(m.fset, f, dstPath)
		for _, p := range slices.Sorted(maps.Keys(m.imports)) {
			astutil.AddImport(m.fset, f, p)
		}
		m.imports = nil
		return true
	}
	return false
}

// mapping is one -map entry: calls of a function or method become calls of
// pkg.fn with the World first, then the receiver (for a method), then the
// arguments.
type mapping struct{ pkg, fn string }

// parseMaps reads -map: 'from=to,...', where from is importpath.Func or
// (importpath.Type).Method and to is importpath.Func.
func parseMaps(spec string) (map[string]mapping, error) {
	out := map[string]mapping{}
	for _, kv := range strings.Split(spec, ",") {
		kv = strings.TrimSpace(kv)
		if kv == "" {
			continue
		}
		from, to, ok := strings.Cut(kv, "=")
		i := strings.LastIndex(to, ".")
		if !ok || i <= 0 || i == len(to)-1 || strings.LastIndex(from, ".") <= 0 {
			return nil, fmt.Errorf("-map %q: want importpath.Func=importpath.Func or (importpath.Type).Method=importpath.Func", kv)
		}
		out[from] = mapping{pkg: to[:i], fn: to[i+1:]}
	}
	return out, nil
}

// mapKey names the function or method a call calls, as -map does, or "".
func (m *migrator) mapKey(s *ast.SelectorExpr) (key string, method bool) {
	fn, ok := m.info.Uses[s.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil {
		return "", false
	}
	sig := fn.Type().(*types.Signature)
	if sig.Recv() == nil {
		return fn.Pkg().Path() + "." + fn.Name(), false
	}
	t := sig.Recv().Type()
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return "", false
	}
	return "(" + named.Obj().Pkg().Path() + "." + named.Obj().Name() + ")." + fn.Name(), true
}

// waitGroupGo rewrites wg.Go(f) on a sync.WaitGroup (Go 1.25) to
// wg.Go(w, f): dst.WaitGroup.Go takes the World that runs f.
func (m *migrator) waitGroupGo(n *ast.CallExpr, s *ast.SelectorExpr) bool {
	if s.Sel.Name != "Go" || len(n.Args) != 1 {
		return false
	}
	fn, ok := m.info.Uses[s.Sel].(*types.Func)
	if !ok || fn.Pkg() == nil || fn.Pkg().Path() != "sync" {
		return false
	}
	recv := fn.Type().(*types.Signature).Recv()
	if recv == nil || !strings.HasSuffix(recv.Type().String(), "sync.WaitGroup") {
		return false
	}
	n.Args = append([]ast.Expr{m.w()}, n.Args...)
	m.changes++
	return true
}

// mapCall rewrites a call that -map names; it reports whether it did.
func (m *migrator) mapCall(c *astutil.Cursor, n *ast.CallExpr, s *ast.SelectorExpr) bool {
	if len(m.maps) == 0 {
		return false
	}
	key, method := m.mapKey(s)
	to, ok := m.maps[key]
	if !ok {
		return false
	}
	pos := n.Pos()
	args := []ast.Expr{m.w()}
	if method {
		args = append(args, s.X)
	}
	name := path.Base(to.pkg)
	if to.pkg == m.pkg.Path() {
		n.Fun = ast.NewIdent(to.fn) // a function of the package being migrated
	} else {
		n.Fun = sel(ast.NewIdent(name), to.fn)
		if m.imports == nil {
			m.imports = map[string]bool{}
		}
		m.imports[to.pkg] = true
	}
	n.Args = append(args, n.Args...)
	setPos(n, pos)
	m.changes++
	return true
}

// skipForTests reports whether, loading with tests, pkg is done by
// another of pkgs: the plain package when its test build (which has the
// package's files and its tests) was loaded too, or the generated main.
func skipForTests(pkg *packages.Package, pkgs []*packages.Package) bool {
	if strings.HasSuffix(pkg.ID, ".test") {
		return true
	}
	if strings.Contains(pkg.ID, " [") {
		return false
	}
	for _, p := range pkgs {
		if strings.HasPrefix(p.ID, pkg.ID+" [") {
			return true
		}
	}
	return false
}

func recvType(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return recvType(t.X)
	case *ast.Ident:
		return t.Name
	case *ast.IndexExpr:
		return recvType(t.X)
	case *ast.IndexListExpr: // a generic receiver with several type parameters
		return recvType(t.X)
	}
	return ""
}

// decl rewrites statements first (they need the original types: select,
// send, receive), then types and calls.
func (m *migrator) decl(d ast.Node) {
	m.markInterceptorConns(d)
	var sigs []*types.Signature // enclosing functions, innermost last
	astutil.Apply(d, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.FuncDecl:
			sig, _ := m.info.Defs[n.Name].Type().(*types.Signature)
			sigs = append(sigs, sig)
		case *ast.FuncLit:
			sig, _ := m.info.TypeOf(n).(*types.Signature)
			sigs = append(sigs, sig)
		}
		return true
	}, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.FuncDecl, *ast.FuncLit:
			sigs = sigs[:len(sigs)-1]
		case *ast.ReturnStmt:
			// return nil where a result is a channel: the zero dst.Chan.
			if len(sigs) > 0 && sigs[len(sigs)-1] != nil {
				res := sigs[len(sigs)-1].Results()
				for i, r := range n.Results {
					if i < res.Len() && isNil(r) {
						if ch, ok := res.At(i).Type().Underlying().(*types.Chan); ok {
							n.Results[i] = &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: expr(m.typeString(ch.Elem()))}}}
							setPos(n, n.Pos())
							m.changes++
						}
					}
				}
			}
		case *ast.SelectStmt:
			m.plans = append(m.plans, m.planSelect(n))
			m.changes++
		case *ast.SendStmt:
			if !inComm(c) && m.isChan(n.Chan) {
				m.replace(c, &ast.ExprStmt{X: call(sel(n.Chan, "Send"), n.Value)})
				m.changes++
			}
		case *ast.AssignStmt:
			// ch = nil: the zero dst.Chan.
			if len(n.Lhs) == len(n.Rhs) {
				for i, r := range n.Rhs {
					if isNil(r) && m.isChan(n.Lhs[i]) {
						n.Rhs[i] = m.zeroChan(n.Lhs[i])
						setPos(n, n.Pos())
						m.changes++
					}
				}
			}
			if len(n.Rhs) == 1 {
				if u, ok := n.Rhs[0].(*ast.UnaryExpr); ok && u.Op == token.ARROW && m.isChan(u.X) {
					if inComm(c) {
						break
					}
					lhs := n.Lhs
					if len(lhs) == 1 {
						lhs = append(lhs, ast.NewIdent("_"))
					}
					n.Lhs, n.Rhs = lhs, []ast.Expr{call(sel(u.X, "Recv"))}
					setPos(n, n.Pos())
					m.changes++
				}
			}
		case *ast.ExprStmt:
			if u, ok := n.X.(*ast.UnaryExpr); ok && u.Op == token.ARROW && m.isChan(u.X) {
				if !inComm(c) {
					n.X = call(sel(u.X, "Recv"))
					setPos(n, n.Pos())
					m.changes++
				}
			}
		case *ast.UnaryExpr:
			if n.Op == token.ARROW && m.isChan(n.X) {
				switch c.Parent().(type) {
				case *ast.ExprStmt, *ast.AssignStmt: // rewritten above
				default:
					m.replace(c, call(ast.NewIdent("dstRecv"), n.X))
					m.needRecv = true
					m.changes++
				}
			}
		case *ast.RangeStmt:
			if m.isChan(n.X) {
				n.X = call(sel(n.X, "All"))
				setPos(n, n.Pos())
				m.changes++
			}
		case *ast.CallExpr:
			m.nilChanArgs(n)
			m.worldParamCall(n)
			m.callExpr(c, n)
		case *ast.CompositeLit:
			m.nilChanElems(n)
			m.fillWorld(n)
		case *ast.BinaryExpr:
			if (n.Op == token.EQL || n.Op == token.NEQ) && (isNil(n.Y) && m.isChan(n.X) || isNil(n.X) && m.isChan(n.Y)) {
				ch := n.X
				if isNil(n.X) {
					ch = n.Y
				}
				n.X, n.Y = ch, m.zeroChan(ch)
				setPos(n, n.Pos())
				m.changes++
			}
		case *ast.GoStmt:
			m.replace(c, m.goStmt(n))
			m.changes++
		}
		return true
	})
	// Types last: make(chan) is rewritten in callExpr, so remaining
	// ChanTypes are in type positions.
	astutil.Apply(d, nil, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.ChanType:
			m.replace(c, &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: n.Value})
			m.changes++
		case *ast.StarExpr:
			// *os.File: the World's dst.File (an interface).
			if s, ok := n.X.(*ast.SelectorExpr); ok && !m.realFS {
				if tn, ok := m.info.Uses[s.Sel].(*types.TypeName); ok && tn.Name() == "File" && tn.Pkg() != nil && tn.Pkg().Path() == "os" {
					m.replace(c, sel(ast.NewIdent("dst"), "File"))
					m.changes++
				}
			}
			// *grpc.ClientConn and *grpc.Server: dstgrpc's (grpc's own in
			// the real World).
			if s, ok := n.X.(*ast.SelectorExpr); ok && !m.keepGRPC[n] {
				if tn, ok := m.info.Uses[s.Sel].(*types.TypeName); ok && tn.Pkg() != nil && tn.Pkg().Path() == grpcPath &&
					(tn.Name() == "ClientConn" || tn.Name() == "Server") {
					if tn.Name() == "ClientConn" {
						m.replace(c, sel(ast.NewIdent("dstgrpc"), "ClientConn")) // an interface
					} else {
						n.X = sel(ast.NewIdent("dstgrpc"), "Server")
					}
					m.need(dstgrpcPath)
					m.changes++
				}
			}
		case *ast.SelectorExpr:
			// http.DefaultClient and DefaultTransport: dsthttp's (the same
			// objects in the real World).
			if v, ok := m.info.Uses[n.Sel].(*types.Var); ok && v.Pkg() != nil && v.Pkg().Path() == "net/http" && (v.Name() == "DefaultClient" || v.Name() == "DefaultTransport") {
				fn := map[string]string{"DefaultClient": "DefaultClient", "DefaultTransport": "Transport"}[v.Name()]
				r := call(sel(ast.NewIdent("dsthttp"), fn), m.w())
				setPos(r, n.Pos())
				m.replace(c, r)
				m.need(dsthttpPath)
				m.changes++
				break
			}
			// crypto/rand.Reader, under any import name: the World's
			// dst.CryptoReader.
			if v, ok := m.info.Uses[n.Sel].(*types.Var); ok && v.Name() == "Reader" && v.Pkg() != nil && v.Pkg().Path() == "crypto/rand" {
				r := call(sel(ast.NewIdent("dst"), "CryptoReader"), m.w())
				setPos(r, n.Pos())
				m.replace(c, r)
				m.changes++
				break
			}
			if id, ok := n.X.(*ast.Ident); ok {
				switch id.Name + "." + n.Sel.Name {
				case "sync.Mutex", "sync.RWMutex", "sync.WaitGroup", "sync.Once", "sync.Cond", "sync.NewCond", "sync.Pool",
					"sync.OnceFunc", "sync.OnceValue", "sync.OnceValues":
					// sync.NewCond(l) is dst.NewCond(l): the Cond fields
					// become dst.Cond, so their constructor must too.
					id.Name = "dst"
					m.changes++
				case "io.PipeReader", "io.PipeWriter":
					if _, isType := m.info.Uses[n.Sel].(*types.TypeName); isType {
						id.Name = "dst"
						m.changes++
					}
				case "time.Timer", "time.Ticker":
					if _, isType := m.info.Uses[n.Sel].(*types.TypeName); isType {
						id.Name = "dst"
						m.changes++
					}
				}
			}
		}
		return true
	})
}

// inComm reports whether c is a select case's communication (not a
// statement in its body).
func inComm(c *astutil.Cursor) bool {
	_, ok := c.Parent().(*ast.CommClause)
	return ok && c.Name() == "Comm"
}

// replace puts n in place of c's node, at its position (so the printer
// keeps comments around it where they were).
func (m *migrator) replace(c *astutil.Cursor, n ast.Node) {
	setPos(n, c.Node().Pos())
	c.Replace(n)
}

func (m *migrator) zeroChan(ch ast.Expr) ast.Expr {
	// Parenthesized: a composite literal in an if or for condition must be.
	return &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: expr(m.chanElem(ch))}}}
}

// nilChanArgs rewrites f(nil) where the parameter is a channel (plain or
// variadic) to the zero dst.Chan.
func (m *migrator) nilChanArgs(n *ast.CallExpr) {
	sig, ok := m.info.TypeOf(n.Fun).(*types.Signature)
	if !ok || m.info.Types[n.Fun].IsType() {
		return // a conversion or a builtin
	}
	params := sig.Params()
	for i, a := range n.Args {
		if !isNil(a) || params.Len() == 0 {
			continue
		}
		var pt types.Type
		switch {
		case sig.Variadic() && i >= params.Len()-1:
			if n.Ellipsis.IsValid() {
				continue
			}
			pt = params.At(params.Len() - 1).Type().(*types.Slice).Elem()
		case i < params.Len():
			pt = params.At(i).Type()
		default:
			continue
		}
		if ch, ok := pt.Underlying().(*types.Chan); ok {
			z := &ast.CompositeLit{Type: &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: expr(m.typeString(ch.Elem()))}}
			setPos(z, a.Pos()) // where nil was, so comments stay put
			n.Args[i] = z
			m.changes++
		}
	}
}

// nilChanElems rewrites nil where a composite literal's field or element
// is a channel (T{ch: nil}, T{nil}, []chan T{nil}) to the zero dst.Chan.
func (m *migrator) nilChanElems(n *ast.CompositeLit) {
	t := m.info.TypeOf(n)
	if t == nil {
		return
	}
	at := func(i int, key ast.Expr) types.Type {
		switch u := t.Underlying().(type) {
		case *types.Struct:
			if id, ok := key.(*ast.Ident); ok {
				for f := range u.Fields() {
					if f.Name() == id.Name {
						return f.Type()
					}
				}
				return nil
			}
			if key == nil && i < u.NumFields() {
				return u.Field(i).Type()
			}
		case *types.Slice:
			return u.Elem()
		case *types.Array:
			return u.Elem()
		case *types.Map:
			return u.Elem()
		}
		return nil
	}
	for i, e := range n.Elts {
		var key ast.Expr
		val := &n.Elts[i]
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			key, val = kv.Key, &kv.Value
		}
		if !isNil(*val) {
			continue
		}
		ft := at(i, key)
		if ft == nil {
			continue
		}
		if ch, ok := ft.Underlying().(*types.Chan); ok {
			z := &ast.CompositeLit{Type: &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: expr(m.typeString(ch.Elem()))}}
			setPos(z, (*val).Pos())
			*val = z
			m.changes++
		}
	}
}

func isNil(e ast.Expr) bool { id, ok := e.(*ast.Ident); return ok && id.Name == "nil" }

func (m *migrator) callExpr(c *astutil.Cursor, n *ast.CallExpr) {
	pos := n.Pos() // before any rewrite replaces n.Fun, which has it
	if id, ok := n.Fun.(*ast.Ident); ok {
		switch id.Name {
		case "make":
			if ct, ok := n.Args[0].(*ast.ChanType); ok {
				size := ast.Expr(&ast.BasicLit{Kind: token.INT, Value: "0"})
				if len(n.Args) > 1 {
					size = n.Args[1]
				}
				m.replace(c, call(&ast.IndexExpr{X: sel(ast.NewIdent("dst"), "MakeChan"), Index: ct.Value}, m.w(), size))
				m.changes++
			}
		case "close":
			if len(n.Args) == 1 && m.isChan(n.Args[0]) {
				m.replace(c, call(sel(n.Args[0], "Close")))
				m.changes++
			}
		case "len", "cap":
			if len(n.Args) == 1 && m.isChan(n.Args[0]) {
				m.replace(c, call(sel(n.Args[0], map[string]string{"len": "Len", "cap": "Cap"}[id.Name])))
				m.changes++
			}
		}
		return
	}
	s, ok := n.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
	if m.mapCall(c, n, s) {
		return
	}
	if m.waitGroupGo(n, s) {
		return
	}
	if !m.realFS {
		m.fileMethod(c, n)
	}
	m.grpcMethod(n, s)
	pkg, ok := s.X.(*ast.Ident)
	if !ok {
		return
	}
	obj, ok := m.info.Uses[pkg].(*types.PkgName)
	if !ok {
		return
	}
	switch obj.Imported().Path() {
	case "time":
		switch s.Sel.Name {
		case "Now", "Since", "Until", "Sleep", "After", "AfterFunc", "NewTimer", "NewTicker", "Tick":
			n.Fun = sel(m.w(), s.Sel.Name)
			setPos(n, pos)
			m.changes++
		}
	case "math/rand":
		// v1's functions, by the same names, on the World's MathRand
		// (math/rand's own functions in the real World: Seed and the
		// GODEBUGs keep their meaning).
		switch s.Sel.Name {
		case "Int", "Intn", "Int31", "Int31n", "Int63", "Int63n", "Uint32", "Uint64", "Float32", "Float64",
			"ExpFloat64", "NormFloat64", "Perm", "Shuffle", "Read", "Seed":
			n.Fun = sel(call(sel(m.w(), "MathRand")), s.Sel.Name)
			setPos(n, pos)
			m.changes++
		}
	case "math/rand/v2":
		switch s.Sel.Name {
		case "Int", "IntN", "Int32", "Int32N", "Int64", "Int64N", "Uint", "UintN", "Uint32", "Uint32N",
			"Uint64", "Uint64N", "Float32", "Float64", "ExpFloat64", "NormFloat64", "Perm", "Shuffle":
			n.Fun = sel(call(sel(m.w(), "Rand")), s.Sel.Name)
			setPos(n, pos)
			m.changes++
		case "N":
			// rand.N(n), generic: dst.N(w.Rand(), n), rand.N itself in the
			// real World.
			n.Fun = sel(ast.NewIdent("dst"), "N")
			n.Args = append([]ast.Expr{call(sel(m.w(), "Rand"))}, n.Args...)
			setPos(n, pos)
			m.changes++
		}
	case "context":
		switch s.Sel.Name {
		case "AfterFunc":
			// context.AfterFunc(ctx, f): dst.AfterDone(ctx, f), which a
			// simulated context's cancel also runs in a goroutine of its own.
			n.Fun = sel(ast.NewIdent("dst"), "AfterDone")
			setPos(n, pos)
			m.changes++
		case "WithCancel", "WithTimeout", "WithDeadline", "WithCancelCause", "WithTimeoutCause", "WithDeadlineCause":
			// The World's (each the std call in the real World): a simulated
			// World waits on its own contexts only.
			n.Fun = sel(ast.NewIdent("dst"), s.Sel.Name)
			n.Args = append([]ast.Expr{m.w()}, n.Args...)
			setPos(n, pos)
			m.changes++
		}
	case "os", "io/ioutil":
		if !m.realFS {
			m.fileCall(n, s, obj.Imported().Path())
		}
	case "path/filepath":
		switch s.Sel.Name {
		case "Walk", "WalkDir", "Glob":
			if !m.realFS {
				// filepath's own in the production World (dst.WalkDir).
				n.Fun = sel(ast.NewIdent("dst"), s.Sel.Name)
				n.Args = append([]ast.Expr{call(sel(m.w(), "FS"))}, n.Args...)
				setPos(n, pos)
				m.changes++
			}
		}
	case "io":
		if s.Sel.Name == "Pipe" {
			r := call(sel(ast.NewIdent("dst"), "Pipe"), m.w())
			setPos(r, pos)
			m.replace(c, r)
			m.changes++
		}
	case "net":
		switch s.Sel.Name {
		case "Dial", "Listen", "ListenPacket":
			n.Fun = sel(call(sel(m.w(), "Net")), s.Sel.Name)
			setPos(n, pos)
			m.changes++
		case "DialTimeout":
			n.Fun = sel(ast.NewIdent("dst"), "DialTimeout")
			n.Args = append([]ast.Expr{m.w()}, n.Args...)
			setPos(n, pos)
			m.changes++
		case "Pipe":
			r := call(sel(ast.NewIdent("dst"), "NetPipe"), m.w())
			setPos(r, pos)
			m.replace(c, r)
			m.changes++
		case "ListenTCP", "DialTCP", "ListenUDP", "DialUDP", "ListenUnix", "DialUnix", "ListenIP", "DialIP", "FileConn", "FileListener":
			m.warn(n, "net."+s.Sel.Name+" has no World equivalent (it returns net's concrete types); use Listen, Dial or ListenPacket")
		}
	case "net/http":
		switch s.Sel.Name {
		case "Get", "Post", "Head", "PostForm":
			n.Fun = sel(call(sel(ast.NewIdent("dsthttp"), "DefaultClient"), m.w()), s.Sel.Name)
			m.need(dsthttpPath)
			setPos(n, pos)
			m.changes++
		case "ListenAndServe", "Serve":
			n.Fun = sel(ast.NewIdent("dsthttp"), s.Sel.Name)
			n.Args = append([]ast.Expr{m.w()}, n.Args...)
			m.need(dsthttpPath)
			setPos(n, pos)
			m.changes++
		case "ListenAndServeTLS", "ServeTLS":
			m.warn(n, "http."+s.Sel.Name+" has no World equivalent yet; it still uses the real network")
		}
	case grpcPath:
		m.grpcCall(n, s, pos)
	case "runtime":
		if s.Sel.Name == "Gosched" {
			// A spin waiting for another goroutine: in a simulation only
			// World.Yield lets that goroutine run. Unless -gosched names
			// the function: a turn for others in a loop that makes its own
			// progress, where Yield would wait for a step no one owes.
			fn := "Yield"
			if m.gosched[m.funcName] {
				fn = "Gosched"
			}
			n.Fun = sel(m.w(), fn)
			setPos(n, pos)
			m.changes++
		}
		switch {
		case s.Sel.Name == "NumCPU":
			n.Fun = sel(m.w(), "NumCPU")
			setPos(n, pos)
			m.changes++
		case s.Sel.Name == "GOMAXPROCS" && len(n.Args) == 1:
			if tv := m.info.Types[n.Args[0]]; tv.Value != nil && tv.Value.String() == "0" {
				n.Fun, n.Args = sel(m.w(), "GOMAXPROCS"), nil
				setPos(n, pos)
				m.changes++
			} else {
				m.warn(n, "runtime.GOMAXPROCS(n) sets the process's count: left as a hint (a simulated World reports its seed's draw; read it with w.GOMAXPROCS())")
			}
		}
	case "crypto/rand":
		switch s.Sel.Name {
		case "Read":
			n.Fun = sel(call(sel(m.w(), "Rand")), "CryptoRead")
			setPos(n, pos)
			m.changes++
		case "Text":
			n.Fun = sel(ast.NewIdent("dst"), "CryptoText")
			n.Args = []ast.Expr{m.w()}
			setPos(n, pos)
			m.changes++
		}
	}
}

// fsMethods are the package os functions the World's FS has, with the
// same signature. Lstat isn't one: the real World's Stat follows
// symbolic links, so code that detects or skips them would change
// meaning in production (it is reported instead).
var fsMethods = map[string]string{"OpenFile": "OpenFile", "Mkdir": "Mkdir", "MkdirAll": "MkdirAll",
	"Remove": "Remove", "RemoveAll": "RemoveAll", "Rename": "Rename", "Stat": "Stat", "ReadDir": "ReadDir"}

// fsHelpers are the os (and io/ioutil) functions package dst has as
// helpers taking the FS first.
var fsHelpers = map[string]string{"Open": "Open", "Create": "Create", "ReadFile": "ReadFile", "WriteFile": "WriteFile", "Link": "Link",
	"Chmod": "Chmod", "Chtimes": "Chtimes", "Truncate": "Truncate", "MkdirTemp": "MkdirTemp", "CreateTemp": "CreateTemp", "DirFS": "DirFS"}

// ioutilHelpers are io/ioutil's functions that are os's under other
// names, mapped to dst's helper for the os one.
var ioutilHelpers = map[string]string{"ReadFile": "ReadFile", "WriteFile": "WriteFile", "TempDir": "MkdirTemp", "TempFile": "CreateTemp"}

// fsMissing are os functions that touch the filesystem but have no World
// equivalent: left as they are, with a warning.
var fsMissing = map[string]bool{"Chown": true, "Lchown": true, "Symlink": true, "Readlink": true, "Chdir": true,
	"Getwd": true, "Lstat": true, "OpenInRoot": true, "OpenRoot": true, "CopyFS": true}

// fileCall rewrites a filesystem call of package os (or io/ioutil) to
// the World's FS: os.Rename(a, b) to w.FS().Rename(a, b), os.Open(name)
// to dst.Open(w.FS(), name). Names are passed through unchanged (the
// production real World's FS is the os package).
func (m *migrator) fileCall(n *ast.CallExpr, s *ast.SelectorExpr, pkg string) {
	name := s.Sel.Name
	pos := n.Pos() // before the rewrite: the new Fun has none
	fsys := func() ast.Expr { return call(sel(m.w(), "FS")) }
	switch {
	case pkg == "os" && fsMethods[name] != "":
		n.Fun = sel(fsys(), fsMethods[name])
	case pkg == "os" && fsHelpers[name] != "":
		n.Fun = sel(ast.NewIdent("dst"), fsHelpers[name])
		n.Args = append([]ast.Expr{fsys()}, n.Args...)
	case pkg == "io/ioutil" && ioutilHelpers[name] != "":
		n.Fun = sel(ast.NewIdent("dst"), ioutilHelpers[name])
		n.Args = append([]ast.Expr{fsys()}, n.Args...)
	case pkg == "io/ioutil" && name == "ReadDir":
		m.warn(n, "ioutil.ReadDir returns FileInfos; use the World's FS ReadDir (DirEntries) by hand")
		return
	case pkg == "os" && fsMissing[name]:
		switch name {
		case "Lstat":
			m.warn(n, "os.Lstat has no World equivalent; it still uses the real filesystem (w.FS().Stat would do where no symbolic link can be: the real World's Stat follows them)")
		default:
			m.warn(n, pkg+"."+name+" has no World equivalent; it still uses the real filesystem")
		}
		return
	default:
		return
	}
	// The generated nodes take the call's position, or the printer lets
	// the comments after the call drift into its argument list.
	setPos(n, pos)
	m.changes++
}

// fileMethod handles a method call on a file the migration makes a
// dst.File: WriteString becomes io.WriteString, and methods dst.File
// lacks are reported.
func (m *migrator) fileMethod(c *astutil.Cursor, n *ast.CallExpr) {
	s, ok := n.Fun.(*ast.SelectorExpr)
	if !ok || !m.isOSFile(m.info.TypeOf(s.X)) {
		return
	}
	switch s.Sel.Name {
	case "Read", "Write", "ReadAt", "WriteAt", "Seek", "Close", "Name", "Stat", "Sync", "Truncate", "ReadDir":
	case "WriteString":
		m.replace(c, call(sel(ast.NewIdent("io"), "WriteString"), append([]ast.Expr{s.X}, n.Args...)...))
		m.changes++
	case "Chmod", "Readdir", "Readdirnames":
		// dst's helpers: the *os.File's own methods in the real World.
		fn := map[string]string{"Chmod": "FileChmod", "Readdir": "Readdir", "Readdirnames": "Readdirnames"}[s.Sel.Name]
		m.replace(c, call(sel(ast.NewIdent("dst"), fn), append([]ast.Expr{s.X}, n.Args...)...))
		m.changes++
	default:
		m.warn(n, "(*os.File)."+s.Sel.Name+" isn't part of dst.File")
	}
}

// isOSFile reports whether t is *os.File.
func (m *migrator) isOSFile(t types.Type) bool {
	p, ok := t.(*types.Pointer)
	if !ok {
		return false
	}
	named, ok := p.Elem().(*types.Named)
	return ok && named.Obj().Name() == "File" && named.Obj().Pkg() != nil && named.Obj().Pkg().Path() == "os"
}

// goStmt: go f(a, b) evaluates f, a and b now; w.Go(func() { f(a, b) })
// would evaluate them later, so whatever could change in between is
// bound first: every operand but constants, function literals,
// functions, and local variables the file never changes (see mutated).
// A method's receiver is bound as Go evaluates it: a pointer or an
// interface is copied; otherwise (a value receiver, which go copies now,
// or a method on an addressable value) the method value itself is bound.
func (m *migrator) goStmt(g *ast.GoStmt) ast.Stmt {
	if fl, ok := g.Call.Fun.(*ast.FuncLit); ok && len(g.Call.Args) == 0 {
		return &ast.ExprStmt{X: call(sel(m.w(), "Go"), fl)}
	}
	var pre []ast.Stmt
	force := func(e ast.Expr) ast.Expr {
		m.selN++
		name := ast.NewIdent(fmt.Sprintf("goArg%d", m.selN))
		pre = append(pre, &ast.AssignStmt{Lhs: []ast.Expr{name}, Tok: token.DEFINE, Rhs: []ast.Expr{e}})
		return ast.NewIdent(name.Name)
	}
	bind := func(e ast.Expr) ast.Expr {
		if m.stable(e) || m.worldArgs[e] {
			return e
		}
		return force(e)
	}
	fn := g.Call.Fun
	switch f := fn.(type) {
	case *ast.FuncLit:
	case *ast.SelectorExpr:
		if id, ok := f.X.(*ast.Ident); ok {
			if _, isPkg := m.info.Uses[id].(*types.PkgName); isPkg {
				break // pkg.F
			}
			if m.info.Uses[id] == nil && (id.Name == "dst" || id.Name == "dsthttp") {
				break // a function this migration just wrote (dsthttp.Serve)
			}
		}
		selection := m.info.Selections[f]
		recvT := m.info.TypeOf(f.X)
		switch {
		case selection != nil && selection.Kind() == types.MethodVal && recvT != nil && (isPointer(recvT) || types.IsInterface(recvT)) && !(isPointer(recvT) && !ptrRecv(selection)):
			fn = &ast.SelectorExpr{X: bind(f.X), Sel: f.Sel} // copy the pointer or interface now
		case m.stable(f.X):
			if selection != nil && selection.Kind() == types.MethodVal && isPointer(recvT) && !ptrRecv(selection) {
				fn = force(f) // *p is copied now
			}
		default:
			fn = force(f) // the method value or the func field, now
		}
	default:
		fn = bind(fn)
	}
	args := make([]ast.Expr, len(g.Call.Args))
	for i, a := range g.Call.Args {
		args[i] = bind(a)
	}
	body := &ast.BlockStmt{List: []ast.Stmt{&ast.ExprStmt{X: &ast.CallExpr{Fun: fn, Args: args, Ellipsis: g.Call.Ellipsis}}}}
	stmt := &ast.ExprStmt{X: call(sel(m.w(), "Go"), &ast.FuncLit{Type: &ast.FuncType{Params: &ast.FieldList{}}, Body: body})}
	if len(pre) == 0 {
		return stmt
	}
	return &ast.BlockStmt{List: append(pre, stmt)}
}

func isPointer(t types.Type) bool { _, ok := t.Underlying().(*types.Pointer); return ok }

// ptrRecv reports whether a method selection's method has a pointer
// receiver.
func ptrRecv(s *types.Selection) bool {
	sig, ok := s.Obj().Type().(*types.Signature)
	return ok && sig.Recv() != nil && isPointer(sig.Recv().Type())
}

// stable reports whether e has the same value when a goroutine started
// now runs as it has now: a constant, a function literal, a function or
// type, nil, or a local variable the file never changes.
func (m *migrator) stable(e ast.Expr) bool {
	if tv, ok := m.info.Types[e]; ok && tv.Value != nil {
		return true
	}
	switch e := e.(type) {
	case *ast.BasicLit, *ast.FuncLit:
		return true
	case *ast.ParenExpr:
		return m.stable(e.X)
	case *ast.Ident:
		switch obj := m.info.Uses[e].(type) {
		case *types.Var:
			return obj.Parent() != nil && obj.Parent() != m.pkg.Scope() && !m.mutated[obj]
		case *types.Nil, *types.Const, *types.Func, *types.TypeName, *types.Builtin:
			return true
		}
	}
	return false
}

// mutatedIn returns the variables f may change after they are declared:
// assigned (including by := alongside a new name), incremented, ranged
// into, address-taken, or given to a pointer method (an implicit &),
// directly or through a field or element.
func (m *migrator) mutatedIn(f *ast.File) map[types.Object]bool {
	mut := map[types.Object]bool{}
	// mark records e's variable as changed. A change through a pointer,
	// or to a slice's or map's element, leaves the variable itself (the
	// pointer, the slice header, the map) as it was: the walk stops there.
	mark := func(e ast.Expr) {
		indirect := func(x ast.Expr) bool {
			t := m.info.TypeOf(x)
			if t == nil {
				return false
			}
			switch t.Underlying().(type) {
			case *types.Pointer, *types.Slice, *types.Map:
				return true
			}
			return false
		}
		for {
			switch x := e.(type) {
			case *ast.Ident:
				if obj := m.info.ObjectOf(x); obj != nil {
					mut[obj] = true
				}
				return
			case *ast.SelectorExpr:
				if indirect(x.X) {
					return
				}
				e = x.X
			case *ast.IndexExpr:
				if indirect(x.X) {
					return
				}
				e = x.X
			case *ast.StarExpr:
				return // *p = v changes what p points to, not p
			case *ast.ParenExpr:
				e = x.X
			case *ast.SliceExpr:
				if indirect(x.X) {
					return
				}
				e = x.X
			default:
				return
			}
		}
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.AssignStmt:
			for _, l := range n.Lhs {
				if id, ok := l.(*ast.Ident); ok && n.Tok == token.DEFINE && m.info.Defs[id] != nil {
					continue // declared here
				}
				mark(l)
			}
		case *ast.IncDecStmt:
			mark(n.X)
		case *ast.RangeStmt:
			if n.Tok == token.ASSIGN {
				if n.Key != nil {
					mark(n.Key)
				}
				if n.Value != nil {
					mark(n.Value)
				}
			}
		case *ast.UnaryExpr:
			if n.Op == token.AND {
				mark(n.X)
			}
		case *ast.SelectorExpr:
			if s := m.info.Selections[n]; s != nil && s.Kind() == types.MethodVal && ptrRecv(s) {
				if t := m.info.TypeOf(n.X); t != nil && !isPointer(t) {
					mark(n.X)
				}
			}
		}
		return true
	})
	return mut
}

// selectPlan is what the text pass needs to rewrite one select that it
// can't get from the syntax alone: the World and the cases' element
// types (from the original type information).
type selectPlan struct {
	pos   token.Pos
	world string
	elems []string // per case: the channel's element type ("" for default)
}

// ctxDone returns the context's source if e is ctx.Done() on a
// context.Context (a select case on it becomes dst.OnDone), else "".
func (m *migrator) ctxDone(e ast.Expr) string {
	c, ok := e.(*ast.CallExpr)
	if !ok || len(c.Args) != 0 {
		return ""
	}
	s, ok := c.Fun.(*ast.SelectorExpr)
	if !ok || s.Sel.Name != "Done" {
		return ""
	}
	if t := m.info.TypeOf(s.X); t == nil || !isContext(t) {
		return ""
	}
	var b bytes.Buffer
	printer.Fprint(&b, m.fset, s.X)
	return b.String()
}

// isContext reports whether t has a context's Done and Err: Done() <-chan
// struct{} and Err() error.
func isContext(t types.Type) bool {
	has := func(name string) *types.Signature {
		obj, _, _ := types.LookupFieldOrMethod(t, true, nil, name)
		fn, ok := obj.(*types.Func)
		if !ok {
			return nil
		}
		return fn.Type().(*types.Signature)
	}
	done, errm := has("Done"), has("Err")
	if done == nil || errm == nil || done.Params().Len() != 0 || done.Results().Len() != 1 {
		return false
	}
	ch, ok := done.Results().At(0).Type().(*types.Chan)
	return ok && ch.Dir() == types.RecvOnly && types.Identical(ch.Elem(), types.NewStruct(nil, nil))
}

func (m *migrator) planSelect(s *ast.SelectStmt) selectPlan {
	p := selectPlan{pos: s.Pos(), world: m.world}
	for _, cl := range s.Body.List {
		elem := ""
		switch comm := cl.(*ast.CommClause).Comm.(type) {
		case *ast.ExprStmt:
			if ctx := m.ctxDone(comm.X.(*ast.UnaryExpr).X); ctx != "" {
				elem = "ctx:" + ctx // case <-ctx.Done(): dst.OnDone
				break
			}
			elem = m.chanElem(comm.X.(*ast.UnaryExpr).X)
		case *ast.AssignStmt:
			elem = m.chanElem(comm.Rhs[0].(*ast.UnaryExpr).X)
		case *ast.SendStmt:
			// Named only when the value has another type (the send converts
			// it): naming it always fails where a variable shadows the
			// type's name (logFuture := &logFuture{...}).
			if !types.Identical(m.info.TypeOf(comm.Value), m.info.TypeOf(comm.Chan).Underlying().(*types.Chan).Elem()) {
				elem = m.chanElem(comm.Chan)
			}
		}
		p.elems = append(p.elems, elem)
	}
	return p
}

// spliceSelects rewrites src's selects (in source order, plans[i] for the
// i-th) as text: a generated header, then a switch whose cases are the
// original case bodies copied verbatim, comments and all. Innermost (last)
// first, re-parsing after each.
func spliceSelects(name string, src []byte, plans []selectPlan) ([]byte, error) {
	for i := len(plans) - 1; i >= 0; i-- {
		fset := token.NewFileSet()
		f, err := parser.ParseFile(fset, name, src, parser.ParseComments)
		if err != nil {
			return nil, err
		}
		var sels []*ast.SelectStmt
		ast.Inspect(f, func(n ast.Node) bool {
			if s, ok := n.(*ast.SelectStmt); ok {
				sels = append(sels, s)
			}
			return true
		})
		if len(sels) != i+1 { // the ones after i are spliced already
			return nil, fmt.Errorf("%d selects left in the rewritten file, want %d (of %d planned)", len(sels), i+1, len(plans))
		}
		off := func(p token.Pos) int { return fset.Position(p).Offset }
		text := func(n ast.Node) string { return string(src[off(n.Pos()):off(n.End())]) }
		s, plan := sels[i], plans[i]
		id := fmt.Sprintf("sel%d", i+1)
		var head, ops, body strings.Builder
		fmt.Fprintf(&head, "{\nvar %s int\n", id)
		clauses := s.Body.List
		for k, cl := range clauses {
			cc := cl.(*ast.CommClause)
			n := k + 1
			end := off(s.Body.Rbrace)
			if k+1 < len(clauses) {
				end = off(clauses[k+1].Pos())
			}
			caseBody := string(src[off(cc.Colon)+1 : end])
			fmt.Fprintf(&body, "case %d:", n)
			switch comm := cc.Comm.(type) {
			case nil:
				fmt.Fprintf(&ops, "dst.Default(func() { %s = %d }),\n", id, n)
			case *ast.SendStmt:
				typeArg := ""
				if plan.elems[k] != "" {
					typeArg = "[" + plan.elems[k] + "]"
				}
				fmt.Fprintf(&ops, "dst.OnSend%s(%s, %s, func() { %s = %d }),\n", typeArg, text(comm.Chan), text(comm.Value), id, n)
			case *ast.ExprStmt:
				if ctx, ok := strings.CutPrefix(plan.elems[k], "ctx:"); ok {
					fmt.Fprintf(&ops, "dst.OnDone(%s, func() { %s = %d }),\n", ctx, id, n)
					break
				}
				fmt.Fprintf(&ops, "dst.OnRecv(%s, func(_ %s, _ bool) { %s = %d }),\n", text(comm.X.(*ast.UnaryExpr).X), plan.elems[k], id, n)
			case *ast.AssignStmt:
				v, ok := fmt.Sprintf("%sv%d", id, n), fmt.Sprintf("%sok%d", id, n)
				fmt.Fprintf(&head, "var %s %s\n", v, plan.elems[k])
				var lhs []string
				for _, l := range comm.Lhs {
					lhs = append(lhs, text(l))
				}
				if len(comm.Lhs) == 2 {
					fmt.Fprintf(&head, "var %s bool\n", ok)
					fmt.Fprintf(&ops, "dst.OnRecv(%s, func(v %s, ok bool) { %s = %d; %s, %s = v, ok }),\n", text(comm.Rhs[0].(*ast.UnaryExpr).X), plan.elems[k], id, n, v, ok)
					fmt.Fprintf(&body, "\n%s %s %s, %s", strings.Join(lhs, ", "), comm.Tok, v, ok)
				} else {
					fmt.Fprintf(&ops, "dst.OnRecv(%s, func(v %s, _ bool) { %s = %d; %s = v }),\n", text(comm.Rhs[0].(*ast.UnaryExpr).X), plan.elems[k], id, n, v)
					fmt.Fprintf(&body, "\n%s %s %s", lhs[0], comm.Tok, v)
				}
			}
			body.WriteString(caseBody)
		}
		// The default never runs (one case always fires); it keeps a
		// select whose cases all return a terminating statement.
		repl := head.String() + "dst.Select(" + plan.world + ",\n" + ops.String() + ")\nswitch " + id + " {\n" + body.String() +
			"default:\npanic(\"dstmigrate: no select case ran\")\n}\n}"
		src = append(append(append([]byte{}, src[:off(s.Pos())]...), repl...), src[off(s.End()):]...)
	}
	return src, nil
}

func funcLit(params *ast.FieldList, stmts ...ast.Stmt) ast.Expr {
	if params == nil {
		params = &ast.FieldList{}
	}
	return &ast.FuncLit{Type: &ast.FuncType{Params: params}, Body: &ast.BlockStmt{List: stmts}}
}

func call(fn ast.Expr, args ...ast.Expr) *ast.CallExpr { return &ast.CallExpr{Fun: fn, Args: args} }
func sel(x ast.Expr, name string) *ast.SelectorExpr {
	return &ast.SelectorExpr{X: x, Sel: ast.NewIdent(name)}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "dstmigrate:", err)
	os.Exit(1)
}

// matchAny reports whether name matches one of the comma-separated
// path.Match patterns.
func matchAny(patterns, name string) bool {
	for _, p := range strings.Split(patterns, ",") {
		if p = strings.TrimSpace(p); p != "" {
			if ok, _ := path.Match(p, name); ok {
				return true
			}
		}
	}
	return false
}
