// Command dstmigrate rewrites a package to run in a dst.World: the
// mechanical part of adopting go-dst-kit.
//
// It loads the package with type information and rewrites, in place:
//
//   - channel types to dst.Chan[T], make(chan T, n) to dst.MakeChan[T](w, n);
//   - sends to c.Send(v), receives to c.Recv() (or dstRecv(c) where only
//     the value is used), range over a channel to range c.All(), close,
//     len and cap of channels to their methods, and a channel compared
//     with or set to nil to the zero dst.Chan;
//   - select statements to dst.Select, keeping their control flow: each
//     case's callback records that it fired (and what it received), and a
//     switch after the select runs the original case bodies, so break,
//     continue and return mean what they did;
//   - time.Now, Since, Until, Sleep, After, AfterFunc, NewTimer and
//     NewTicker to the World's methods, *time.Timer and *time.Ticker to
//     *dst.Timer and *dst.Ticker;
//   - sync.Mutex, RWMutex, WaitGroup, Once and Cond to their dst types;
//   - go statements to w.Go, evaluating the call's arguments first as go
//     does;
//   - math/rand calls to the World's Rand.
//
// The World expression is chosen by the enclosing method's receiver:
// -world 'Raft=$.w,NetworkTransport=$.w' uses r.w in methods of Raft
// whose receiver is named r. Elsewhere it writes __w, which won't
// compile: each such place needs a World passed in by hand. Review the
// result: the rewrite is mechanical, and code relying on raw channel
// identity, reflection over channels, or channels from other packages
// needs a person.
//
// Usage: dstmigrate [-world MAP] [-n] packages...
package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"strings"

	"golang.org/x/tools/go/ast/astutil"
	"golang.org/x/tools/go/packages"
	"golang.org/x/tools/imports"
)

const dstPath = "github.com/prince776/go-dst-kit/dst"

func main() {
	worldFlag := flag.String("world", "", "receiver type -> World expression, $ for the receiver: 'Raft=$.w,Pool=$.world'")
	dryRun := flag.Bool("n", false, "print the files instead of writing them")
	flag.Parse()
	worlds := map[string]string{}
	for _, kv := range strings.Split(*worldFlag, ",") {
		if k, v, ok := strings.Cut(strings.TrimSpace(kv), "="); ok {
			worlds[k] = v
		}
	}
	cfg := &packages.Config{Mode: packages.NeedName | packages.NeedFiles | packages.NeedCompiledGoFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo, Tests: false}
	pkgs, err := packages.Load(cfg, flag.Args()...)
	if err != nil {
		fatal(err)
	}
	if packages.PrintErrors(pkgs) > 0 {
		os.Exit(1)
	}
	for _, pkg := range pkgs {
		m := &migrator{fset: pkg.Fset, info: pkg.TypesInfo, worlds: worlds}
		for i, f := range pkg.Syntax {
			name := pkg.CompiledGoFiles[i]
			if !m.file(f) {
				continue
			}
			var buf bytes.Buffer
			if err := format.Node(&buf, pkg.Fset, f); err != nil {
				var raw bytes.Buffer
				printer.Fprint(&raw, pkg.Fset, f)
				fatal(fmt.Errorf("%s: %w\n%s", name, err, raw.String()))
			}
			out, err := imports.Process(name, buf.Bytes(), nil)
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
		if m.needRecv {
			fmt.Fprintf(os.Stderr, "%s: add the helper (once per package):\n\tfunc dstRecv[T any](c dst.Chan[T]) T { v, _ := c.Recv(); return v }\n", pkg.PkgPath)
		}
	}
}

type migrator struct {
	fset     *token.FileSet
	info     *types.Info
	worlds   map[string]string
	changes  int
	needRecv bool
	world    string // the World expression in the current function
	selN     int
}

func (m *migrator) isChan(e ast.Expr) bool {
	t := m.info.TypeOf(e)
	if t == nil {
		return false
	}
	_, ok := t.Underlying().(*types.Chan)
	return ok
}

func (m *migrator) chanElem(e ast.Expr) string {
	return types.TypeString(m.info.TypeOf(e).Underlying().(*types.Chan).Elem(), m.qualifier)
}

func (m *migrator) qualifier(p *types.Package) string { return p.Name() }

func expr(src string) ast.Expr {
	e, err := parseExpr(src)
	if err != nil {
		panic(fmt.Sprintf("dstmigrate: bad generated expression %q: %v", src, err))
	}
	return e
}

func (m *migrator) w() ast.Expr { return expr(m.world) }

// file rewrites f; it reports whether anything changed.
func (m *migrator) file(f *ast.File) bool {
	before := m.changes
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		m.world = "__w"
		if ok && fd.Recv != nil && len(fd.Recv.List) == 1 {
			tname := recvType(fd.Recv.List[0].Type)
			if tmpl, ok := m.worlds[tname]; ok && len(fd.Recv.List[0].Names) == 1 {
				m.world = strings.ReplaceAll(tmpl, "$", fd.Recv.List[0].Names[0].Name)
			}
		}
		m.decl(d)
	}
	if m.changes > before {
		astutil.AddImport(m.fset, f, dstPath)
		return true
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
	}
	return ""
}

// decl rewrites statements first (they need the original types: select,
// send, receive), then types and calls.
func (m *migrator) decl(d ast.Node) {
	astutil.Apply(d, nil, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.SelectStmt:
			c.Replace(m.selectStmt(n))
			m.changes++
		case *ast.SendStmt:
			if !inComm(c) {
				c.Replace(&ast.ExprStmt{X: call(sel(n.Chan, "Send"), n.Value)})
				m.changes++
			}
		case *ast.AssignStmt:
			if len(n.Rhs) == 1 {
				if u, ok := n.Rhs[0].(*ast.UnaryExpr); ok && u.Op == token.ARROW {
					if inComm(c) {
						break
					}
					lhs := n.Lhs
					if len(lhs) == 1 {
						lhs = append(lhs, ast.NewIdent("_"))
					}
					n.Lhs, n.Rhs = lhs, []ast.Expr{call(sel(u.X, "Recv"))}
					m.changes++
				}
			}
		case *ast.ExprStmt:
			if u, ok := n.X.(*ast.UnaryExpr); ok && u.Op == token.ARROW {
				if !inComm(c) {
					n.X = call(sel(u.X, "Recv"))
					m.changes++
				}
			}
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
				switch c.Parent().(type) {
				case *ast.ExprStmt, *ast.AssignStmt: // rewritten above
				default:
					c.Replace(call(ast.NewIdent("dstRecv"), n.X))
					m.needRecv = true
					m.changes++
				}
			}
		case *ast.RangeStmt:
			if m.isChan(n.X) {
				n.X = call(sel(n.X, "All"))
				m.changes++
			}
		case *ast.CallExpr:
			m.callExpr(c, n)
		case *ast.BinaryExpr:
			if (n.Op == token.EQL || n.Op == token.NEQ) && (isNil(n.Y) && m.isChan(n.X) || isNil(n.X) && m.isChan(n.Y)) {
				ch := n.X
				if isNil(n.X) {
					ch = n.Y
				}
				n.X, n.Y = ch, m.zeroChan(ch)
				m.changes++
			}
		case *ast.GoStmt:
			c.Replace(m.goStmt(n))
			m.changes++
		}
		return true
	})
	// Types last: make(chan) is rewritten in callExpr, so remaining
	// ChanTypes are in type positions.
	astutil.Apply(d, nil, func(c *astutil.Cursor) bool {
		switch n := c.Node().(type) {
		case *ast.ChanType:
			c.Replace(&ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: n.Value})
			m.changes++
		case *ast.SelectorExpr:
			if id, ok := n.X.(*ast.Ident); ok {
				switch id.Name + "." + n.Sel.Name {
				case "sync.Mutex", "sync.RWMutex", "sync.WaitGroup", "sync.Once", "sync.Cond":
					id.Name = "dst"
					m.changes++
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

func (m *migrator) zeroChan(ch ast.Expr) ast.Expr {
	// Parenthesized: a composite literal in an if or for condition must be.
	return &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: expr(m.chanElem(ch))}}}
}

func isNil(e ast.Expr) bool { id, ok := e.(*ast.Ident); return ok && id.Name == "nil" }

func (m *migrator) callExpr(c *astutil.Cursor, n *ast.CallExpr) {
	if id, ok := n.Fun.(*ast.Ident); ok {
		switch id.Name {
		case "make":
			if ct, ok := n.Args[0].(*ast.ChanType); ok {
				size := ast.Expr(&ast.BasicLit{Kind: token.INT, Value: "0"})
				if len(n.Args) > 1 {
					size = n.Args[1]
				}
				c.Replace(call(&ast.IndexExpr{X: sel(ast.NewIdent("dst"), "MakeChan"), Index: ct.Value}, m.w(), size))
				m.changes++
			}
		case "close":
			if len(n.Args) == 1 && m.isChan(n.Args[0]) {
				c.Replace(call(sel(n.Args[0], "Close")))
				m.changes++
			}
		case "len", "cap":
			if len(n.Args) == 1 && m.isChan(n.Args[0]) {
				c.Replace(call(sel(n.Args[0], map[string]string{"len": "Len", "cap": "Cap"}[id.Name])))
				m.changes++
			}
		}
		return
	}
	s, ok := n.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}
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
		case "Now", "Since", "Until", "Sleep", "After", "AfterFunc", "NewTimer", "NewTicker":
			n.Fun = sel(m.w(), s.Sel.Name)
			m.changes++
		}
	case "math/rand", "math/rand/v2":
		switch s.Sel.Name {
		case "Int", "Intn", "IntN", "Int63", "Int63n", "Int64", "Int64N", "Uint32", "Uint64", "Float64", "Perm", "Shuffle":
			name := map[string]string{"Intn": "IntN", "Int63": "Int64", "Int63n": "Int64N"}[s.Sel.Name]
			if name == "" {
				name = s.Sel.Name
			}
			n.Fun = sel(call(sel(m.w(), "Rand")), name)
			m.changes++
		}
	case "crypto/rand":
		if s.Sel.Name == "Read" {
			n.Fun = sel(call(sel(m.w(), "Rand")), "CryptoRead")
			m.changes++
		}
	}
}

// goStmt: go f(a, b) evaluates f, a and b now; w.Go(func() { f(a, b) })
// would evaluate them later, so they are bound first.
func (m *migrator) goStmt(g *ast.GoStmt) ast.Stmt {
	if fl, ok := g.Call.Fun.(*ast.FuncLit); ok && len(g.Call.Args) == 0 {
		return &ast.ExprStmt{X: call(sel(m.w(), "Go"), fl)}
	}
	var pre []ast.Stmt
	bind := func(e ast.Expr) ast.Expr {
		switch e.(type) {
		case *ast.Ident, *ast.BasicLit:
			return e
		}
		m.selN++
		name := ast.NewIdent(fmt.Sprintf("goArg%d", m.selN))
		pre = append(pre, &ast.AssignStmt{Lhs: []ast.Expr{name}, Tok: token.DEFINE, Rhs: []ast.Expr{e}})
		return ast.NewIdent(name.Name)
	}
	fn := g.Call.Fun
	if s, ok := fn.(*ast.SelectorExpr); ok {
		fn = &ast.SelectorExpr{X: bind(s.X), Sel: s.Sel} // the method value's receiver, now
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

// selectStmt rewrites a select into dst.Select plus a switch on the case
// that fired.
func (m *migrator) selectStmt(s *ast.SelectStmt) ast.Stmt {
	m.selN++
	which := fmt.Sprintf("sel%d", m.selN)
	var decls, cases []ast.Stmt
	bindings := map[*ast.CaseClause]int{} // generated statements at the top of each case
	var ops []ast.Expr
	decls = append(decls, mustStmt(fmt.Sprintf("var %s int", which)))
	for i, cl := range s.Body.List {
		cc := cl.(*ast.CommClause)
		k := i + 1
		mark := mustStmt(fmt.Sprintf("%s = %d", which, k))
		var pre []ast.Stmt // bindings at the top of the case body
		switch comm := cc.Comm.(type) {
		case nil: // default
			ops = append(ops, call(sel(ast.NewIdent("dst"), "Default"), funcLit(nil, mark)))
		case *ast.SendStmt:
			ops = append(ops, call(sel(ast.NewIdent("dst"), "OnSend"), m.moved(comm.Chan), m.moved(comm.Value), funcLit(nil, mark)))
		case *ast.ExprStmt: // case <-ch:
			ch := comm.X.(*ast.UnaryExpr).X
			ops = append(ops, call(sel(ast.NewIdent("dst"), "OnRecv"), m.moved(ch), m.recvFunc(ch, mark, nil, nil)))
		case *ast.AssignStmt: // case v := <-ch, v, ok := <-ch, v = <-ch
			ch := comm.Rhs[0].(*ast.UnaryExpr).X
			elem := m.chanElem(ch)
			vName, okName := fmt.Sprintf("%sv%d", which, k), fmt.Sprintf("%sok%d", which, k)
			decls = append(decls, mustStmt(fmt.Sprintf("var %s %s", vName, elem)))
			got := []string{vName}
			if len(comm.Lhs) == 2 {
				decls = append(decls, mustStmt(fmt.Sprintf("var %s bool", okName)))
				got = append(got, okName)
			}
			ops = append(ops, call(sel(ast.NewIdent("dst"), "OnRecv"), m.moved(ch), m.recvFunc(ch, mark, &vName, map[bool]*string{true: &okName, false: nil}[len(comm.Lhs) == 2])))
			rhs := []ast.Expr{ast.NewIdent(vName)}
			if len(comm.Lhs) == 2 {
				rhs = append(rhs, ast.NewIdent(okName))
			}
			pre = append(pre, &ast.AssignStmt{Lhs: comm.Lhs, Tok: comm.Tok, Rhs: rhs})
		}
		clause := &ast.CaseClause{List: []ast.Expr{&ast.BasicLit{Kind: token.INT, Value: fmt.Sprint(k)}}, Body: append(pre, cc.Body...)}
		bindings[clause] = len(pre)
		cases = append(cases, clause)
	}
	selectCall := &ast.ExprStmt{X: call(sel(ast.NewIdent("dst"), "Select"), append([]ast.Expr{m.w()}, ops...)...)}
	sw := &ast.SwitchStmt{Tag: ast.NewIdent(which), Body: &ast.BlockStmt{List: cases}}
	// The generated header takes the select's position, before every
	// original case body, so comments in the bodies stay where they were.
	for _, d := range decls {
		setPos(d, s.Select)
	}
	setPos(selectCall, s.Select)
	for _, c := range cases {
		cc := c.(*ast.CaseClause)
		cc.Case, cc.Colon = s.Select, s.Select
		for _, st := range cc.Body[:bindings[cc]] {
			setPos(st, s.Select)
		}
	}
	return &ast.BlockStmt{List: append(append(decls, selectCall), sw)}
}

// moved copies an expression moved out of place, without its positions
// (the printer places comments by position).
func (m *migrator) moved(e ast.Expr) ast.Expr {
	var b bytes.Buffer
	if err := printer.Fprint(&b, m.fset, e); err != nil {
		panic(err)
	}
	return expr(b.String())
}

func (m *migrator) recvFunc(ch ast.Expr, mark ast.Stmt, v, ok *string) ast.Expr {
	vp, okp := "_", "_"
	body := []ast.Stmt{mark}
	if v != nil {
		vp = "v"
		body = append(body, mustStmt(*v+" = v"))
	}
	if ok != nil {
		okp = "ok"
		body = append(body, mustStmt(*ok+" = ok"))
	}
	return &ast.FuncLit{
		Type: &ast.FuncType{Params: &ast.FieldList{List: []*ast.Field{
			{Names: []*ast.Ident{ast.NewIdent(vp)}, Type: expr(m.chanElem(ch))},
			{Names: []*ast.Ident{ast.NewIdent(okp)}, Type: ast.NewIdent("bool")},
		}}},
		Body: &ast.BlockStmt{List: body},
	}
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
