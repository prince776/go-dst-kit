// Package dstlint is an analyzer that finds code in simulated packages
// that would escape the World: raw goroutines, channels, select, the
// clock, sync primitives that wait, context timers, global randomness, and
// the os, net, net/http and gRPC entry points.
//
// A package opts in with a comment in any of its files:
//
//	//dst:simulated
//
// Every use of dst.Current (the calling goroutine's World) is reported
// in every package, opted in or not: it is a last resort, for leaf code
// that no World can reach. Excuse each use with //dst:ok and the reason.
//
// A line can be excused with a comment on it or the line above:
//
//	mu.Lock() //dst:ok guards a cache no goroutine waits on
//
// Run it with `go run github.com/prince776/go-dst-kit/dstlint/cmd/dstlint ./...`
// or as a go vet tool.
//
// Verified:
//   - [flags-escapes] Every construct in the escape list is reported, with
//     its World replacement, in an opted-in package.
//   - [respects-opt-in] Packages without //dst:simulated aren't reported,
//     except for dst.Current.
//   - [flags-current] Every use of dst.Current is reported, in every
//     package: it is the last resort for code no World reaches.
//   - [respects-ok] A //dst:ok comment on or above a line suppresses its
//     report.
//   - [flags-shared-primitive] In an opted-in package, a package-level
//     variable that is, or holds (in a struct or array, at any depth), a
//     dst.Mutex, RWMutex, WaitGroup, Once or Cond is reported: every
//     simulation running at once (a sweep's parallel seeds) shares it,
//     and the World fails them as misuse when they do.
//   - [flags-pooled-world-value] In an opted-in package, a pool
//     (dst.Pool or sync.Pool) whose New makes a value holding a
//     dst.Chan, Timer or Ticker (through pointers and struct fields, at
//     any depth) is reported: the pool can hand the value to code in
//     another World, where its channel would wait outside that World.
//   - [flags-unset-world] In an opted-in package's non-test files, a keyed
//     literal of a struct with a *dst.World field that leaves it unset is
//     reported when a World is at hand in the function (from a parameter,
//     the receiver or a local, by up to three field selections; not the
//     variable of the literal or of one it is nested in), unless the function sets that variable's
//     World later, by assignment or through a setter method of the
//     package. Such a literal falls back to the real World inside a
//     simulation.
//
// Depends on: golang.org/x/tools/go/analysis
package dstlint

import (
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// Analyzer reports World escapes in packages marked //dst:simulated.
var Analyzer = &analysis.Analyzer{
	Name: "dstlint",
	Doc:  "report code in //dst:simulated packages that escapes the dst World",
	Run:  run,
}

// funcs maps package path -> function name -> the World replacement.
var funcs = map[string]map[string]string{
	"time": {
		"Now": "w.Now()", "Sleep": "w.Sleep", "Since": "w.Since", "Until": "w.Until",
		"After": "w.After", "Tick": "w.Tick", "NewTimer": "w.NewTimer",
		"NewTicker": "w.NewTicker", "AfterFunc": "w.AfterFunc",
	},
	"context": {
		"WithTimeout": "dst.WithTimeout", "WithDeadline": "dst.WithDeadline",
		"WithTimeoutCause": "dst.WithTimeoutCause", "WithDeadlineCause": "dst.WithDeadlineCause",
		"WithCancel": "dst.WithCancel", "WithCancelCause": "dst.WithCancelCause",
		"AfterFunc": "dst.AfterDone",
	},
	"math/rand": {
		"Int": "w.MathRand()", "Intn": "w.MathRand()", "Int63": "w.MathRand()", "Int63n": "w.MathRand()",
		"Int31": "w.MathRand()", "Int31n": "w.MathRand()", "Uint32": "w.MathRand()", "Uint64": "w.MathRand()",
		"Float64": "w.MathRand()", "Float32": "w.MathRand()", "Perm": "w.MathRand()", "Shuffle": "w.MathRand()",
		"Read": "w.MathRand()", "ExpFloat64": "w.MathRand()", "NormFloat64": "w.MathRand()", "Seed": "w.MathRand()",
	},
	"math/rand/v2": {
		"Int": "w.Rand()", "IntN": "w.Rand()", "Int64": "w.Rand()", "Int64N": "w.Rand()",
		"Int32": "w.Rand()", "Int32N": "w.Rand()", "Uint32": "w.Rand()", "Uint64": "w.Rand()",
		"Uint32N": "w.Rand()", "Uint64N": "w.Rand()", "UintN": "w.Rand()", "N": "dst.N(w.Rand(), n)", "Uint": "w.Rand()",
		"Float64": "w.Rand()", "Float32": "w.Rand()", "Perm": "w.Rand()", "Shuffle": "w.Rand()",
		"ExpFloat64": "w.Rand()", "NormFloat64": "w.Rand()",
	},
	"io":          {"Pipe": "dst.Pipe(w)"},
	"sync":        {"NewCond": "dst.NewCond", "OnceFunc": "dst.OnceFunc", "OnceValue": "dst.OnceValue", "OnceValues": "dst.OnceValues"},
	"runtime":     {"Gosched": "w.Yield", "NumCPU": "w.NumCPU", "GOMAXPROCS": "w.GOMAXPROCS to read it (a set is a hint a simulation ignores: excuse it)"},
	"crypto/rand": {"Read": "w.Rand().CryptoRead", "Int": "crypto/rand.Int(dst.CryptoReader(w), max)", "Text": "dst.CryptoText(w)"},
	"os": {
		"Open": "dst.Open(w.FS(), ...)", "Create": "dst.Create(w.FS(), ...)", "OpenFile": "w.FS().OpenFile",
		"ReadFile": "dst.ReadFile(w.FS(), ...)", "WriteFile": "dst.WriteFile(w.FS(), ...)",
		"Remove": "w.FS().Remove", "RemoveAll": "w.FS().RemoveAll", "Rename": "w.FS().Rename",
		"Mkdir": "w.FS().Mkdir", "MkdirAll": "w.FS().MkdirAll", "ReadDir": "w.FS().ReadDir",
		"Stat": "w.FS().Stat", "Lstat": "w.FS().Stat where no symbolic link can be (the real World's Stat follows them)", "Truncate": "dst.Truncate(w.FS(), ...)", "Link": "dst.Link(w.FS(), ...)",
		"Chmod": "dst.Chmod(w.FS(), ...)", "Chtimes": "dst.Chtimes(w.FS(), ...)", "MkdirTemp": "dst.MkdirTemp(w.FS(), ...)",
		"CreateTemp": "dst.CreateTemp(w.FS(), ...)", "DirFS": "dst.DirFS(w.FS(), dir)",
	},
	"path/filepath": {"Walk": "dst.Walk(w.FS(), ...)", "WalkDir": "dst.WalkDir(w.FS(), ...)", "Glob": "dst.Glob(w.FS(), ...)"},
	"io/ioutil":     {"ReadFile": "dst.ReadFile(w.FS(), ...)", "WriteFile": "dst.WriteFile(w.FS(), ...)", "TempDir": "dst.MkdirTemp(w.FS(), ...)", "TempFile": "dst.CreateTemp(w.FS(), ...)"},
	"net": {
		"Dial": "w.Net().Dial", "DialTimeout": "dst.DialTimeout(w, ...)", "Listen": "w.Net().Listen", "Pipe": "dst.NetPipe(w)",
		"ListenTCP": "w.Net().Listen", "DialTCP": "w.Net().Dial", "ListenPacket": "w.Net().ListenPacket",
		"ListenUDP": "w.Net().ListenPacket", "DialUDP": "w.Net().Dial",
	},
	"net/http": {
		"ListenAndServe": "dsthttp.ListenAndServe(w, ...)", "Serve": "dsthttp.Serve(w, ...)", "Get": "dsthttp.DefaultClient(w).Get",
		"Post": "dsthttp.DefaultClient(w).Post", "Head": "dsthttp.DefaultClient(w).Head", "PostForm": "dsthttp.DefaultClient(w).PostForm",
	},
	"google.golang.org/grpc": {
		"NewServer": "dstgrpc.NewServerExact(w, ...)", "NewClient": "dstgrpc.NewClientExact(w, ...)", "Dial": "grpc.NewClient, as dstgrpc.NewClientExact(w, ...)",
		"DialContext": "grpc.NewClient, as dstgrpc.NewClientExact(w, ...)",
	},
}

// types maps package path -> type name -> the World replacement.
var typeNames = map[string]map[string]string{
	"sync": {
		"Mutex": "dst.Mutex", "RWMutex": "dst.RWMutex", "WaitGroup": "dst.WaitGroup",
		"Once": "dst.Once", "Cond": "dst.Cond", "Pool": "dst.Pool",
	},
	"time":     {"Timer": "dst.Timer", "Ticker": "dst.Ticker"},
	"net/http": {"Server": "dsthttp.Serve"},
	"net":      {"Dialer": "w.Net().DialContext"},
}

// vars maps package path -> variable name -> the World replacement.
var vars = map[string]map[string]string{
	"net/http":    {"DefaultClient": "dsthttp.DefaultClient(w)", "DefaultTransport": "dsthttp.Transport(w)"},
	"crypto/rand": {"Reader": "dst.CryptoReader(w)"},
}

const dstPath = "github.com/prince776/go-dst-kit/dst"

func run(pass *analysis.Pass) (any, error) {
	simulated := optedIn(pass.Files)
	var setters map[types.Object]bool
	if simulated {
		setters = worldSetters(pass)
	}
	for _, f := range pass.Files {
		ok := okLines(pass.Fset, f)
		report := func(pos token.Pos, format string, args ...any) {
			line := pass.Fset.Position(pos).Line
			if ok[line] || ok[line-1] {
				return
			}
			pass.Reportf(pos, format, args...)
		}
		if simulated {
			unsetWorld(pass, f, setters, report)
			sharedPrimitives(pass, f, report)
			pooledWorldValues(pass, f, report)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, isSel := n.(*ast.SelectorExpr); isSel {
				if obj := pass.TypesInfo.Uses[sel.Sel]; obj != nil && obj.Pkg() != nil && obj.Pkg().Path() == dstPath && obj.Name() == "Current" {
					if _, isFunc := obj.(*types.Func); isFunc {
						report(sel.Pos(), "dst.Current is a last resort: pass a World where one is reachable (excuse with //dst:ok and why none is)")
					}
				}
			}
			if !simulated {
				return true
			}
			switch n := n.(type) {
			case *ast.GoStmt:
				report(n.Pos(), "go statement escapes the World; use w.Go")
			case *ast.SelectStmt:
				report(n.Pos(), "select statement escapes the World; use dst.Select")
			case *ast.SendStmt:
				report(n.Pos(), "channel send escapes the World; use a dst.Chan")
			case *ast.UnaryExpr:
				if n.Op == token.ARROW {
					report(n.Pos(), "channel receive escapes the World; use a dst.Chan")
				}
			case *ast.RangeStmt:
				if t := pass.TypesInfo.TypeOf(n.X); t != nil {
					if _, isChan := t.Underlying().(*types.Chan); isChan {
						report(n.Pos(), "range over a channel escapes the World; use dst.Chan.All")
					}
				}
			case *ast.CallExpr:
				if id, isIdent := n.Fun.(*ast.Ident); isIdent && id.Name == "make" && len(n.Args) > 0 {
					if t := pass.TypesInfo.TypeOf(n.Args[0]); t != nil {
						if _, isChan := t.Underlying().(*types.Chan); isChan {
							report(n.Pos(), "make(chan) escapes the World; use dst.MakeChan")
						}
					}
				}
			case *ast.SelectorExpr:
				obj := pass.TypesInfo.Uses[n.Sel]
				if obj == nil || obj.Pkg() == nil {
					return true
				}
				path, name := obj.Pkg().Path(), obj.Name()
				switch obj.(type) {
				case *types.Func:
					if sig, _ := obj.Type().(*types.Signature); sig != nil && sig.Recv() != nil {
						return true // methods are judged by their receiver's type
					}
					if repl, found := funcs[path][name]; found {
						report(n.Pos(), "%s.%s escapes the World; use %s", lastElem(path), name, repl)
					}
				case *types.TypeName:
					if repl, found := typeNames[path][name]; found {
						report(n.Pos(), "%s.%s escapes the World; use %s", lastElem(path), name, repl)
					}
				case *types.Var:
					if repl, found := vars[path][name]; found {
						report(n.Pos(), "%s.%s escapes the World; use %s", lastElem(path), name, repl)
					}
				}
			}
			return true
		})
	}
	return nil, nil
}

func optedIn(files []*ast.File) bool {
	for _, f := range files {
		for _, cg := range f.Comments {
			for _, c := range cg.List {
				if strings.TrimSpace(c.Text) == "//dst:simulated" {
					return true
				}
			}
		}
	}
	return false
}

func okLines(fset *token.FileSet, f *ast.File) map[int]bool {
	ok := map[int]bool{}
	for _, cg := range f.Comments {
		for _, c := range cg.List {
			if strings.HasPrefix(c.Text, "//dst:ok") {
				ok[fset.Position(c.Pos()).Line] = true
			}
		}
	}
	return ok
}

func lastElem(path string) string {
	if path == "google.golang.org/grpc" {
		return "grpc"
	}
	if first, _, _ := strings.Cut(path, "/"); !strings.Contains(first, ".") {
		return path // the standard library: crypto/rand, not rand
	}
	if i := strings.LastIndex(path, "/"); i >= 0 && !strings.HasPrefix(path[i+1:], "v") {
		return path[i+1:]
	}
	return path
}

// waitingPrimitives are the dst types goroutines wait on, which one
// simulation can't share with another running at once.
var waitingPrimitives = map[string]bool{"Mutex": true, "RWMutex": true, "WaitGroup": true, "Once": true, "Cond": true}

// sharedPrimitives reports f's package-level variables that are, or
// hold, a primitive goroutines wait on ([flags-shared-primitive]).
func sharedPrimitives(pass *analysis.Pass, f *ast.File, report func(token.Pos, string, ...any)) {
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.VAR {
			continue
		}
		for _, spec := range g.Specs {
			for _, name := range spec.(*ast.ValueSpec).Names {
				obj := pass.TypesInfo.Defs[name]
				if obj == nil || name.Name == "_" {
					continue
				}
				if what := heldPrimitive(obj.Type(), 0); what != "" {
					report(name.Pos(), "package-level %s (in %s): every simulation running at once (a sweep's parallel seeds) shares it, "+
						"which fails them as misuse; make it per instance (a field of the system under test)", what, name.Name)
				}
			}
		}
	}
}

// heldPrimitive names the waiting dst primitive t is or holds by value
// (or through a *dst.Cond, as NewCond makes), or "".
func heldPrimitive(t types.Type, depth int) string {
	if depth > 8 {
		return ""
	}
	if p, ok := t.(*types.Pointer); ok {
		if n, ok := p.Elem().(*types.Named); ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == dstPath && n.Obj().Name() == "Cond" {
			return "dst.Cond"
		}
		return ""
	}
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == dstPath && waitingPrimitives[n.Obj().Name()] {
		return "dst." + n.Obj().Name()
	}
	switch u := t.Underlying().(type) {
	case *types.Struct:
		for i := range u.NumFields() {
			if w := heldPrimitive(u.Field(i).Type(), depth+1); w != "" {
				return w
			}
		}
	case *types.Array:
		return heldPrimitive(u.Elem(), depth+1)
	}
	return ""
}

// pooledWorldValues reports pools whose New makes a value holding a
// World's channel or timer ([flags-pooled-world-value]).
func pooledWorldValues(pass *analysis.Pass, f *ast.File, report func(token.Pos, string, ...any)) {
	isPool := func(t types.Type) bool {
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		n, ok := t.(*types.Named)
		if !ok || n.Obj().Pkg() == nil || n.Obj().Name() != "Pool" {
			return false
		}
		path := n.Obj().Pkg().Path()
		return path == dstPath || path == "sync"
	}
	check := func(fn ast.Expr) {
		lit, ok := fn.(*ast.FuncLit)
		if !ok {
			return
		}
		ast.Inspect(lit.Body, func(n ast.Node) bool {
			if _, nested := n.(*ast.FuncLit); nested {
				return false
			}
			ret, ok := n.(*ast.ReturnStmt)
			if !ok {
				return true
			}
			for _, r := range ret.Results {
				if what := worldBound(pass.TypesInfo.TypeOf(r), map[types.Type]bool{}); what != "" {
					report(r.Pos(), "a pooled value holding a %s: the pool can hand it to code in another World, where it would wait outside that World; "+
						"keep the World in the value and remake the %s when the World changes (docs/migrating.md)", what, what)
				}
			}
			return true
		})
	}
	ast.Inspect(f, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CompositeLit:
			if t := pass.TypesInfo.TypeOf(n); t != nil && isPool(t) {
				for _, e := range n.Elts {
					if kv, ok := e.(*ast.KeyValueExpr); ok {
						if k, ok := kv.Key.(*ast.Ident); ok && k.Name == "New" {
							check(kv.Value)
						}
					}
				}
			}
		case *ast.AssignStmt:
			for i, l := range n.Lhs {
				if s, ok := l.(*ast.SelectorExpr); ok && s.Sel.Name == "New" && i < len(n.Rhs) {
					if t := pass.TypesInfo.TypeOf(s.X); t != nil && isPool(t) {
						check(n.Rhs[i])
					}
				}
			}
		}
		return true
	})
}

// worldBound names the World-bound dst value (a Chan, Timer or Ticker)
// t is or holds, through pointers and struct fields, or "".
func worldBound(t types.Type, seen map[types.Type]bool) string {
	if t == nil || seen[t] {
		return ""
	}
	seen[t] = true
	if p, ok := t.(*types.Pointer); ok {
		return worldBound(p.Elem(), seen)
	}
	if n, ok := t.(*types.Named); ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == dstPath {
		switch n.Obj().Name() {
		case "Chan", "Timer", "Ticker":
			return "dst." + n.Obj().Name()
		}
	}
	switch u := t.Underlying().(type) {
	case *types.Struct:
		for i := range u.NumFields() {
			if w := worldBound(u.Field(i).Type(), seen); w != "" {
				return w
			}
		}
	case *types.Array:
		return worldBound(u.Elem(), seen)
	}
	return ""
}
