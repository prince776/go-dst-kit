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
	"go/parser"
	"go/printer"
	"go/token"
	"go/types"
	"os"
	"sort"
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
		m := &migrator{pkg: pkg.Types, fset: pkg.Fset, info: pkg.TypesInfo, worlds: worlds}
		for i, f := range pkg.Syntax {
			name := pkg.CompiledGoFiles[i]
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
		if m.needRecv {
			fmt.Fprintf(os.Stderr, "%s: add the helper (once per package):\n\tfunc dstRecv[T any](c dst.Chan[T]) T { v, _ := c.Recv(); return v }\n", pkg.PkgPath)
		}
	}
}

type migrator struct {
	pkg      *types.Package
	fset     *token.FileSet
	info     *types.Info
	worlds   map[string]string
	changes  int
	needRecv bool
	world    string       // the World expression in the current function
	plans    []selectPlan // the file's selects, in source order, for the text pass
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
							n.Results[i] = &ast.ParenExpr{X: &ast.CompositeLit{Type: &ast.IndexExpr{X: sel(ast.NewIdent("dst"), "Chan"), Index: expr(types.TypeString(ch.Elem(), m.qualifier))}}}
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
			if !inComm(c) {
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
				if u, ok := n.Rhs[0].(*ast.UnaryExpr); ok && u.Op == token.ARROW {
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
			if u, ok := n.X.(*ast.UnaryExpr); ok && u.Op == token.ARROW {
				if !inComm(c) {
					n.X = call(sel(u.X, "Recv"))
					setPos(n, n.Pos())
					m.changes++
				}
			}
		case *ast.UnaryExpr:
			if n.Op == token.ARROW {
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
			m.callExpr(c, n)
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
			setPos(n, n.Pos())
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
			setPos(n, n.Pos())
			m.changes++
		}
	case "crypto/rand":
		if s.Sel.Name == "Read" {
			n.Fun = sel(call(sel(m.w(), "Rand")), "CryptoRead")
			setPos(n, n.Pos())
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

// selectPlan is what the text pass needs to rewrite one select that it
// can't get from the syntax alone: the World and the cases' element
// types (from the original type information).
type selectPlan struct {
	pos   token.Pos
	world string
	elems []string // per case: the channel's element type ("" for default)
}

func (m *migrator) planSelect(s *ast.SelectStmt) selectPlan {
	p := selectPlan{pos: s.Pos(), world: m.world}
	for _, cl := range s.Body.List {
		elem := ""
		switch comm := cl.(*ast.CommClause).Comm.(type) {
		case *ast.ExprStmt:
			elem = m.chanElem(comm.X.(*ast.UnaryExpr).X)
		case *ast.AssignStmt:
			elem = m.chanElem(comm.Rhs[0].(*ast.UnaryExpr).X)
		case *ast.SendStmt:
			elem = m.chanElem(comm.Chan) // explicit: a send converts its value to it
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
				fmt.Fprintf(&ops, "dst.OnSend[%s](%s, %s, func() { %s = %d }),\n", plan.elems[k], text(comm.Chan), text(comm.Value), id, n)
			case *ast.ExprStmt:
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
