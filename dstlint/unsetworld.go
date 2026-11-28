package dstlint

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"strings"

	"golang.org/x/tools/go/analysis"
)

// unsetWorld reports [flags-unset-world]: in a simulated package's
// non-test files, a keyed struct literal whose type has a *dst.World
// field, leaving it unset, while a *dst.World is at hand in the enclosing
// function (reachable from a parameter, the receiver or a local by up to
// three field selections; the variable of the literal, or of a literal
// it is nested in, doesn't count). A
// literal bound to a variable whose World field the function sets later,
// by assignment or by calling a setter on it (a method of this package
// whose body sets its receiver's World field, or calls such a setter on
// its receiver), is not reported.

const reachDepth = 3

func isWorldPtr(t types.Type) bool {
	p, ok := t.(*types.Pointer)
	if !ok {
		return false
	}
	n, ok := p.Elem().(*types.Named)
	return ok && n.Obj().Pkg() != nil && n.Obj().Pkg().Path() == dstPath && n.Obj().Name() == "World"
}

func structOf(t types.Type) *types.Struct {
	if p, ok := t.(*types.Pointer); ok {
		t = p.Elem()
	}
	s, _ := t.Underlying().(*types.Struct)
	return s
}

// worldFields returns the names of t's *dst.World fields (direct only).
func worldFields(t types.Type) []string {
	s := structOf(t)
	if s == nil {
		return nil
	}
	var names []string
	for i := 0; i < s.NumFields(); i++ {
		if isWorldPtr(s.Field(i).Type()) {
			names = append(names, s.Field(i).Name())
		}
	}
	return names
}

// reach returns a selector path from expr (of type t) to a *dst.World,
// or "".
func reach(pkg *types.Package, expr string, t types.Type, depth int) string {
	if isWorldPtr(t) {
		return expr
	}
	if depth == 0 {
		return ""
	}
	s := structOf(t)
	if s == nil {
		return ""
	}
	for i := 0; i < s.NumFields(); i++ {
		f := s.Field(i)
		if !f.Exported() && f.Pkg() != pkg {
			continue
		}
		if isWorldPtr(f.Type()) {
			return expr + "." + f.Name()
		}
	}
	for i := 0; i < s.NumFields(); i++ {
		f := s.Field(i)
		if !f.Exported() && f.Pkg() != pkg {
			continue
		}
		if p := reach(pkg, expr+"."+f.Name(), f.Type(), depth-1); p != "" {
			return p
		}
	}
	return ""
}

// worldSetters returns the package's methods that set their receiver's
// World field: directly, or by calling another such method on it.
func worldSetters(pass *analysis.Pass) map[types.Object]bool {
	type method struct {
		obj  types.Object
		recv types.Object
		body *ast.BlockStmt
	}
	var ms []method
	for _, f := range pass.Files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Recv == nil || fd.Body == nil || len(fd.Recv.List) == 0 || len(fd.Recv.List[0].Names) == 0 {
				continue
			}
			ms = append(ms, method{pass.TypesInfo.Defs[fd.Name], pass.TypesInfo.Defs[fd.Recv.List[0].Names[0]], fd.Body})
		}
	}
	setters := map[types.Object]bool{}
	for changed := true; changed; {
		changed = false
		for _, m := range ms {
			if m.obj == nil || m.recv == nil || setters[m.obj] {
				continue
			}
			found := false
			ast.Inspect(m.body, func(n ast.Node) bool {
				switch n := n.(type) {
				case *ast.AssignStmt:
					for _, l := range n.Lhs {
						if sel, ok := l.(*ast.SelectorExpr); ok {
							if t := pass.TypesInfo.TypeOf(sel); t != nil && isWorldPtr(t) {
								if id := rootIdent(sel.X); id != nil && pass.TypesInfo.ObjectOf(id) == m.recv {
									found = true
								}
							}
						}
					}
				case *ast.CallExpr:
					if sel, ok := n.Fun.(*ast.SelectorExpr); ok && setters[pass.TypesInfo.Uses[sel.Sel]] {
						if id := rootIdent(sel.X); id != nil && pass.TypesInfo.ObjectOf(id) == m.recv {
							found = true
						}
					}
				}
				return !found
			})
			if found {
				setters[m.obj], changed = true, true
			}
		}
	}
	return setters
}

func unsetWorld(pass *analysis.Pass, f *ast.File, setters map[types.Object]bool, report func(token.Pos, string, ...any)) {
	if strings.HasSuffix(pass.Fset.Position(f.Pos()).Filename, "_test.go") {
		return
	}
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Body == nil {
			continue
		}
		// What is at hand: receiver, parameters, locals.
		roots := map[types.Object]string{} // a root at hand -> its path to a World
		var order []types.Object
		consider := func(id *ast.Ident) {
			if id == nil || id.Name == "_" {
				return
			}
			if obj := pass.TypesInfo.Defs[id]; obj != nil {
				if v, ok := obj.(*types.Var); ok {
					if _, seen := roots[obj]; seen {
						return
					}
					if p := reach(pass.Pkg, id.Name, v.Type(), reachDepth); p != "" {
						roots[obj] = p
						order = append(order, obj)
					}
				}
			}
		}
		for _, fl := range []*ast.FieldList{fd.Recv, fd.Type.Params} {
			if fl == nil {
				continue
			}
			for _, fld := range fl.List {
				for _, n := range fld.Names {
					consider(n)
				}
			}
		}
		later := map[types.Object]bool{} // variables whose World field is set later
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.Ident:
				consider(n)
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok && setters[pass.TypesInfo.Uses[sel.Sel]] {
					if id := rootIdent(sel.X); id != nil {
						later[pass.TypesInfo.ObjectOf(id)] = true
					}
				}
			case *ast.AssignStmt:
				for _, l := range n.Lhs {
					if sel, ok := l.(*ast.SelectorExpr); ok {
						if t := pass.TypesInfo.TypeOf(sel); t != nil && isWorldPtr(t) {
							if id := rootIdent(sel.X); id != nil {
								later[pass.TypesInfo.ObjectOf(id)] = true
							}
						}
					}
				}
			}
			return true
		})
		if len(order) == 0 {
			continue
		}
		// Literals, with the variable each is bound to (if any).
		bound := map[*ast.CompositeLit]types.Object{}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			bind := func(lhs ast.Expr, rhs ast.Expr) {
				if u, ok := rhs.(*ast.UnaryExpr); ok && u.Op == token.AND {
					rhs = u.X
				}
				if cl, ok := rhs.(*ast.CompositeLit); ok {
					if id := rootIdent(lhs); id != nil {
						bound[cl] = pass.TypesInfo.ObjectOf(id)
					}
				}
			}
			switch n := n.(type) {
			case *ast.AssignStmt:
				if len(n.Lhs) == len(n.Rhs) {
					for i := range n.Lhs {
						bind(n.Lhs[i], n.Rhs[i])
					}
				}
			case *ast.ValueSpec:
				if len(n.Names) == len(n.Values) {
					for i := range n.Names {
						bind(n.Names[i], n.Values[i])
					}
				}
			}
			return true
		})
		// The variables of the literals each literal is nested in.
		within := map[*ast.CompositeLit]map[types.Object]bool{}
		for outer, v := range bound {
			ast.Inspect(outer, func(n ast.Node) bool {
				if inner, ok := n.(*ast.CompositeLit); ok && inner != outer {
					if within[inner] == nil {
						within[inner] = map[types.Object]bool{}
					}
					within[inner][v] = true
				}
				return true
			})
		}
		ast.Inspect(fd.Body, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}
			t := pass.TypesInfo.TypeOf(cl)
			if t == nil {
				return true
			}
			fields := worldFields(t)
			if len(fields) == 0 {
				return true
			}
			if len(cl.Elts) > 0 {
				if _, keyed := cl.Elts[0].(*ast.KeyValueExpr); !keyed {
					return true // unkeyed: every field is set
				}
			}
			set := map[string]bool{}
			for _, e := range cl.Elts {
				if kv, ok := e.(*ast.KeyValueExpr); ok {
					if k, ok := kv.Key.(*ast.Ident); ok {
						set[k.Name] = true
					}
				}
			}
			if obj := bound[cl]; obj != nil && later[obj] {
				return true
			}
			// A World at hand through any root but the literal's own
			// variable, or an enclosing literal's (which this literal is
			// about to set up).
			at := ""
			for _, o := range order {
				if o != bound[cl] && !within[cl][o] {
					at = roots[o]
					break
				}
			}
			if at == "" {
				return true
			}
			for _, name := range fields {
				if !set[name] {
					report(cl.Pos(), "%s literal leaves %s unset while %s is at hand: it falls back to the real World", typeName(t), name, at)
					break
				}
			}
			return true
		})
	}
}

func rootIdent(e ast.Expr) *ast.Ident {
	for {
		switch x := e.(type) {
		case *ast.Ident:
			return x
		case *ast.SelectorExpr:
			e = x.X
		case *ast.StarExpr:
			e = x.X
		case *ast.ParenExpr:
			e = x.X
		case *ast.IndexExpr:
			e = x.X
		default:
			return nil
		}
	}
}

func typeName(t types.Type) string {
	return fmt.Sprint(types.TypeString(t, func(p *types.Package) string { return p.Name() }))
}
