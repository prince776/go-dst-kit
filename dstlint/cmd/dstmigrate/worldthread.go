package main

import (
	"bytes"
	"go/ast"
	"go/printer"
	"go/token"
	"go/types"
	"regexp"
	"strconv"
	"strings"
)

// Threading the World through code that has none (-world-param,
// -world-field), and into the literals of types that keep one.

// worldThreading is the per-run configuration of the three.
type worldThreading struct {
	// params are the functions (Func, Type.Method) that take the World as
	// a new first parameter, w; a value is set once one is met.
	params map[string]bool
	// fields maps a config type to its World field (-world-field
	// 'Config.World'): a function given that type takes its World from
	// the field, or the real World when it's nil. A value is set once the
	// type is met.
	fields map[string]string
	met    map[string]bool
}

func parseWorldThreading(params, fields string) worldThreading {
	wt := worldThreading{params: map[string]bool{}, fields: map[string]string{}, met: map[string]bool{}}
	for _, p := range strings.Split(params, ",") {
		if p = strings.TrimSpace(p); p != "" {
			wt.params[p] = false
		}
	}
	for _, f := range strings.Split(fields, ",") {
		if t, fld, ok := strings.Cut(strings.TrimSpace(f), "."); ok {
			wt.fields[t] = fld
		}
	}
	return wt
}

// funcKey names a function as -world-param does: Func, or Type.Method.
func funcKey(fn *types.Func) string {
	sig, _ := fn.Type().(*types.Signature)
	if sig != nil && sig.Recv() != nil {
		t := sig.Recv().Type()
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		if n, ok := t.(*types.Named); ok {
			return n.Obj().Name() + "." + fn.Name()
		}
	}
	return fn.Name()
}

// declaresW reports whether fd already declares a name w (a parameter,
// a variable, anything), which a World named w would collide with.
func (m *migrator) declaresW(fd *ast.FuncDecl) bool {
	found := false
	ast.Inspect(fd, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && id.Name == "w" && m.info.Defs[id] != nil {
			found = true
		}
		return !found
	})
	return found
}

// worldParam gives fd (a -world-param function) the World as its first
// parameter, w. It reports whether it did.
func (m *migrator) worldParam(fd *ast.FuncDecl) bool {
	if _, listed := m.threading.params[m.funcName]; !listed {
		return false
	}
	m.threading.params[m.funcName] = true
	if m.declaresW(fd) {
		m.warn(fd, m.funcName+" already declares w: -world-param can't add the World as w; add it by hand")
		return false
	}
	name := "w"
	if l := fd.Type.Params; len(l.List) > 0 && m.fset.Position(l.List[0].Pos()).Line > m.fset.Position(l.Opening).Line {
		// A parameter list on its own lines: spliceWorld puts w on its
		// own line too.
		name = paramMarker
	}
	field := &ast.Field{Names: []*ast.Ident{ast.NewIdent(name)}, Type: &ast.StarExpr{X: sel(ast.NewIdent("dst"), "World")}}
	fd.Type.Params.List = append([]*ast.Field{field}, fd.Type.Params.List...)
	m.changes++
	return true
}

// worldParamCall passes the World at hand to a call of a -world-param
// function.
func (m *migrator) worldParamCall(n *ast.CallExpr) {
	var id *ast.Ident
	switch f := ast.Unparen(n.Fun).(type) {
	case *ast.Ident:
		id = f
	case *ast.SelectorExpr:
		id = f.Sel
	case *ast.IndexExpr: // generic instantiation
		if i, ok := f.X.(*ast.Ident); ok {
			id = i
		}
	}
	if id == nil {
		return
	}
	fn, ok := m.info.Uses[id].(*types.Func)
	if !ok {
		return
	}
	if _, listed := m.threading.params[funcKey(fn)]; !listed {
		return
	}
	w := m.w()
	if m.worldArgs == nil {
		m.worldArgs = map[ast.Expr]bool{}
	}
	m.worldArgs[w] = true
	n.Args = append([]ast.Expr{w}, n.Args...)
	m.changes++
}

// configParam is the World expression a function gets from a parameter
// of a -world-field type ("conf.World"), or "".
func (m *migrator) configParam(fd *ast.FuncDecl) string {
	if fd.Body == nil || len(m.threading.fields) == 0 {
		return ""
	}
	for _, f := range fd.Type.Params.List {
		t := m.info.TypeOf(f.Type)
		if p, ok := t.(*types.Pointer); ok {
			t = p.Elem()
		}
		n, ok := t.(*types.Named)
		if !ok || n.Obj().Pkg() != m.pkg {
			continue
		}
		if fld, ok := m.threading.fields[n.Obj().Name()]; ok {
			for _, name := range f.Names {
				if name.Name != "_" {
					return name.Name + "." + fld
				}
			}
		}
	}
	return ""
}

// worldFallback starts fd's body with the World from the config field
// (expression from), the real World when it's nil: a marker statement
// here, which spliceWorld writes out, so the body's comments stay where
// they were and a blank line follows.
func (m *migrator) worldFallback(fd *ast.FuncDecl, from string) {
	mark := &ast.ExprStmt{X: &ast.CallExpr{Fun: &ast.Ident{Name: fallbackMarker, NamePos: fd.Body.Lbrace + 1}, Args: []ast.Expr{ast.NewIdent(from)}, Lparen: fd.Body.Lbrace + 1, Rparen: fd.Body.Lbrace + 1}}
	fd.Body.List = append([]ast.Stmt{mark}, fd.Body.List...)
	m.need(realworldPath)
	m.changes++
}

const (
	paramMarker    = "__dstWorldParam"
	fallbackMarker = "__dstWorldFallback"
	fillMarker     = "__dstWorldFill"
	fieldMarker    = "__dstWorldField"
)

var (
	paramRE     = regexp.MustCompile(`(?m)^(\t+)` + paramMarker + ` \*dst\.World, `)
	paramOpenRE = regexp.MustCompile(`\(` + paramMarker + ` \*dst\.World,\n(\t+)`)
	fieldRE     = regexp.MustCompile(`(?m)^(\t+)` + fieldMarker + `(\w+)\s+\*dst\.World\n`)
	fallbackRE  = regexp.MustCompile(`(?m)^(\t+)` + fallbackMarker + `\((.*)\)\n`)
	fillRE      = regexp.MustCompile(`\{` + fillMarker + `(\d+),\s*`)
)

// spliceWorld writes out the markers worldParam, worldFallback and
// fillWorld left in the printed file src.
func (m *migrator) spliceWorld(src []byte) []byte {
	src = paramRE.ReplaceAll(src, []byte("${1}w *dst.World,\n${1}"))
	src = paramOpenRE.ReplaceAll(src, []byte("(\n${1}w *dst.World,\n${1}"))
	src = bytes.ReplaceAll(src, []byte(paramMarker), []byte("w"))
	src = fieldRE.ReplaceAll(src, []byte("\n${1}// ${2} is the World this runs in (go-dst-kit): its goroutines,\n${1}// timers, clock, channels and randomness. Nil is the real World.\n${1}${2} *dst.World\n"))
	src = fallbackRE.ReplaceAll(src, []byte("${1}w := ${2}\n${1}if w == nil {\n${1}\tw = realworld.Default()\n${1}}\n\n"))
	for {
		loc := fillRE.FindSubmatchIndex(src)
		if loc == nil {
			return src
		}
		i, _ := strconv.Atoi(string(src[loc[2]:loc[3]]))
		// The literal's elements are on lines of their own: so is this one,
		// indented as the next (gofmt aligns the keys after).
		lineStart := bytes.LastIndexByte(src[:loc[0]], '\n') + 1
		indent := leadingTabs(src[lineStart:]) + "\t"
		repl := "{\n" + indent + m.fills[i] + ",\n" + indent
		src = append(src[:loc[0]:loc[0]], append([]byte(repl), src[loc[1]:]...)...)
	}
}

func leadingTabs(b []byte) string {
	n := 0
	for n < len(b) && b[n] == '\t' {
		n++
	}
	return string(b[:n])
}

// addWorldFields gives each -world-field type declared in f the World
// field, when it hasn't one.
func (m *migrator) addWorldFields(f *ast.File) {
	for _, d := range f.Decls {
		g, ok := d.(*ast.GenDecl)
		if !ok || g.Tok != token.TYPE {
			continue
		}
		for _, s := range g.Specs {
			ts := s.(*ast.TypeSpec)
			fld, ok := m.threading.fields[ts.Name.Name]
			if !ok || m.info.Defs[ts.Name] == nil || m.info.Defs[ts.Name].Pkg() != m.pkg {
				continue
			}
			m.threading.met[ts.Name.Name] = true
			st, ok := ts.Type.(*ast.StructType)
			if !ok {
				continue
			}
			has := false
			for _, x := range st.Fields.List {
				for _, n := range x.Names {
					has = has || n.Name == fld
				}
			}
			if has {
				continue
			}
			// A marker spliceWorld writes out with its doc comment, after a
			// blank line.
			st.Fields.List = append(st.Fields.List, &ast.Field{Names: []*ast.Ident{{Name: fieldMarker + fld, NamePos: st.Fields.Closing - 1}}, Type: &ast.StarExpr{Star: st.Fields.Closing - 1, X: sel(ast.NewIdent("dst"), "World")}})
			m.changes++
		}
	}
}

// worldFieldOf is the field a -world template reads the World from
// ("$.w" and "dstworld.Or($.world)" both name one), or "".
func worldFieldOf(tmpl string) string {
	if inner, ok := strings.CutSuffix(tmpl, ")"); ok {
		if i := strings.Index(inner, "("); i > 0 {
			tmpl = inner[i+1:]
		}
	}
	f, ok := strings.CutPrefix(tmpl, "$.")
	if !ok || strings.ContainsAny(f, ".()[] ") {
		return ""
	}
	return f
}

// fillWorld sets the World field in a literal of a type that keeps its
// World there (-world 'T=$.w'), when the literal leaves it unset and a
// World is at hand: through one embedded struct (Raft{raftState:
// raftState{w: w}}) when the field is promoted.
func (m *migrator) fillWorld(n *ast.CompositeLit) {
	if m.world == "__w" {
		return
	}
	t := m.info.TypeOf(n)
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	named, ok := t.(*types.Named)
	if !ok || named.Obj().Pkg() != m.pkg {
		return
	}
	fld := worldFieldOf(m.worlds[named.Obj().Name()])
	st, ok := named.Underlying().(*types.Struct)
	if fld == "" || !ok || setsOwnWorld(named) {
		return
	}
	for _, e := range n.Elts {
		if _, keyed := e.(*ast.KeyValueExpr); !keyed {
			return // positional: every field is set
		}
	}
	keys := map[string]bool{}
	for _, e := range n.Elts {
		if id, ok := e.(*ast.KeyValueExpr).Key.(*ast.Ident); ok {
			keys[id.Name] = true
		}
	}
	var elt ast.Expr
	for i := range st.NumFields() {
		f := st.Field(i)
		if f.Name() == fld && isWorld(f.Type()) {
			if keys[fld] {
				return
			}
			elt = &ast.KeyValueExpr{Key: ast.NewIdent(fld), Value: m.w()}
			break
		}
	}
	if elt == nil {
		for i := range st.NumFields() {
			f := st.Field(i)
			en, ok := f.Type().(*types.Named)
			if !f.Embedded() || !ok || en.Obj().Pkg() != m.pkg {
				continue
			}
			if es, ok := en.Underlying().(*types.Struct); ok {
				for j := range es.NumFields() {
					if es.Field(j).Name() == fld && isWorld(es.Field(j).Type()) {
						if keys[f.Name()] {
							return // set in the embedded literal, or deliberately not
						}
						inner := &ast.CompositeLit{Type: ast.NewIdent(en.Obj().Name()), Elts: []ast.Expr{&ast.KeyValueExpr{Key: ast.NewIdent(fld), Value: m.w()}}}
						elt = &ast.KeyValueExpr{Key: ast.NewIdent(f.Name()), Value: inner}
					}
				}
			}
			if elt != nil {
				break
			}
		}
	}
	if elt == nil {
		return
	}
	if len(n.Elts) > 0 && m.fset.Position(n.Elts[0].Pos()).Line > m.fset.Position(n.Lbrace).Line {
		// Elements on lines of their own: a marker spliceWorld writes out
		// on a line of its own.
		var b strings.Builder
		printer.Fprint(&b, token.NewFileSet(), elt)
		m.fills = append(m.fills, b.String())
		elt = &ast.Ident{Name: fillMarker + strconv.Itoa(len(m.fills)-1), NamePos: n.Lbrace + 1}
	}
	n.Elts = append([]ast.Expr{elt}, n.Elts...)
	m.changes++
}

// setsOwnWorld reports whether t has a method taking a World (an
// init(w)): its literals leave the field to it.
func setsOwnWorld(t *types.Named) bool {
	ms := types.NewMethodSet(types.NewPointer(t))
	for i := range ms.Len() {
		sig, ok := ms.At(i).Type().(*types.Signature)
		if !ok {
			continue
		}
		for j := range sig.Params().Len() {
			if isWorld(sig.Params().At(j).Type()) {
				return true
			}
		}
	}
	return false
}

func isWorld(t types.Type) bool {
	p, ok := t.(*types.Pointer)
	if !ok {
		return false
	}
	n, ok := p.Elem().(*types.Named)
	return ok && n.Obj().Name() == "World" && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == dstPath
}
