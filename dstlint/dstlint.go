// Package dstlint is an analyzer that finds code in simulated packages
// that would escape the World: raw goroutines, channels, select, the
// clock, sync primitives that wait, context timers, global randomness, and
// the os, net, net/http and gRPC entry points.
//
// A package opts in with a comment in any of its files:
//
//	//dst:simulated
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
//   - [respects-opt-in] Packages without //dst:simulated aren't reported.
//   - [respects-ok] A //dst:ok comment on or above a line suppresses its
//     report.
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
		"After": "w.After", "Tick": "w.NewTicker", "NewTimer": "w.NewTimer",
		"NewTicker": "w.NewTicker", "AfterFunc": "w.AfterFunc",
	},
	"context": {
		"WithTimeout": "dst.WithTimeout", "WithDeadline": "dst.WithDeadline",
		"WithTimeoutCause": "dst.WithTimeout", "WithDeadlineCause": "dst.WithDeadline",
		"WithCancel": "dst.WithCancel", "WithCancelCause": "dst.WithCancel",
		"AfterFunc": "dst.AfterDone",
	},
	"math/rand": {
		"Int": "w.Rand()", "Intn": "w.Rand()", "Int63": "w.Rand()", "Int63n": "w.Rand()",
		"Int31": "w.Rand()", "Int31n": "w.Rand()", "Uint32": "w.Rand()", "Uint64": "w.Rand()",
		"Float64": "w.Rand()", "Float32": "w.Rand()", "Perm": "w.Rand()", "Shuffle": "w.Rand()",
		"Read": "w.Rand().CryptoRead", "ExpFloat64": "w.Rand()", "NormFloat64": "w.Rand()",
	},
	"math/rand/v2": {
		"Int": "w.Rand()", "IntN": "w.Rand()", "Int64": "w.Rand()", "Int64N": "w.Rand()",
		"Int32": "w.Rand()", "Int32N": "w.Rand()", "Uint32": "w.Rand()", "Uint64": "w.Rand()",
		"Uint32N": "w.Rand()", "Uint64N": "w.Rand()", "UintN": "w.Rand()", "N": "w.Rand()",
		"Float64": "w.Rand()", "Float32": "w.Rand()", "Perm": "w.Rand()", "Shuffle": "w.Rand()",
		"ExpFloat64": "w.Rand()", "NormFloat64": "w.Rand()",
	},
	"sync":        {"NewCond": "dst.NewCond"},
	"crypto/rand": {"Read": "w.Rand().CryptoRead", "Int": "crypto/rand.Int(dst.CryptoReader(w), max)", "Text": "w.Rand().CryptoRead"},
	"os": {
		"Open": "dst.Open(w.FS(), ...)", "Create": "dst.Create(w.FS(), ...)", "OpenFile": "w.FS().OpenFile",
		"ReadFile": "dst.ReadFile(w.FS(), ...)", "WriteFile": "dst.WriteFile(w.FS(), ...)",
		"Remove": "w.FS().Remove", "RemoveAll": "w.FS().RemoveAll", "Rename": "w.FS().Rename",
		"Mkdir": "w.FS().Mkdir", "MkdirAll": "w.FS().MkdirAll", "ReadDir": "w.FS().ReadDir",
		"Stat": "w.FS().Stat", "Lstat": "w.FS().Stat", "Truncate": "File.Truncate",
	},
	"net": {
		"Dial": "w.Net().Dial", "DialTimeout": "w.Net().DialContext", "Listen": "w.Net().Listen",
		"ListenTCP": "w.Net().Listen", "DialTCP": "w.Net().Dial", "ListenPacket": "w.Net().ListenPacket",
		"ListenUDP": "w.Net().ListenPacket", "DialUDP": "w.Net().Dial",
	},
	"net/http": {
		"ListenAndServe": "dsthttp.Serve", "Serve": "dsthttp.Serve", "Get": "dsthttp.Client(w).Get",
		"Post": "dsthttp.Client(w).Post", "Head": "dsthttp.Client(w).Head", "PostForm": "dsthttp.Client(w).PostForm",
	},
	"google.golang.org/grpc": {
		"NewServer": "dstgrpc.NewServer", "NewClient": "dstgrpc.NewClient", "Dial": "dstgrpc.NewClient",
		"DialContext": "dstgrpc.NewClient",
	},
}

// types maps package path -> type name -> the World replacement.
var typeNames = map[string]map[string]string{
	"sync": {
		"Mutex": "dst.Mutex", "RWMutex": "dst.RWMutex", "WaitGroup": "dst.WaitGroup",
		"Once": "dst.Once", "Cond": "dst.Cond",
	},
	"time":     {"Timer": "dst.Timer", "Ticker": "dst.Ticker"},
	"net/http": {"Server": "dsthttp.Serve"},
	"net":      {"Dialer": "w.Net().DialContext"},
}

// vars maps package path -> variable name -> the World replacement.
var vars = map[string]map[string]string{
	"net/http":    {"DefaultClient": "dsthttp.Client(w)", "DefaultTransport": "dsthttp.Transport(w)"},
	"crypto/rand": {"Reader": "dst.CryptoReader(w)"},
}

func run(pass *analysis.Pass) (any, error) {
	if !optedIn(pass.Files) {
		return nil, nil
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
		ast.Inspect(f, func(n ast.Node) bool {
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
