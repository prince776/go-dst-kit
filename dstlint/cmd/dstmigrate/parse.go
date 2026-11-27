package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"reflect"
)

var tokenFset = token.NewFileSet()

func parseExpr(src string) (ast.Expr, error) {
	e, err := parser.ParseExpr(src)
	if err == nil {
		zeroPos(e)
	}
	return e, err
}

// zeroPos clears every position in n: generated code has none, so the
// printer lays it out afresh instead of by another file's lines.
func zeroPos(n ast.Node) { setPos(n, token.NoPos) }

// setPos sets every unset position in n to p (zeroPos: every position).
func setPos(n ast.Node, p token.Pos) {
	all := p == token.NoPos
	ast.Inspect(n, func(n ast.Node) bool {
		if n == nil {
			return false
		}
		v := reflect.ValueOf(n)
		if v.Kind() == reflect.Pointer {
			v = v.Elem()
		}
		if v.Kind() != reflect.Struct {
			return true
		}
		for i := range v.NumField() {
			// Ellipsis is the one position with a meaning: set, the call
			// spreads its last argument (f(xs...)).
			// (and a declaration's parentheses: set, it prints var (...)).
			if name := v.Type().Field(i).Name; name == "Ellipsis" || (name == "Lparen" || name == "Rparen") && v.Type() == reflect.TypeOf(ast.GenDecl{}) {
				continue
			}
			if f := v.Field(i); f.Type() == reflect.TypeOf(token.NoPos) && f.CanSet() && (all || f.Int() == 0) {
				f.SetInt(int64(p))
			}
		}
		return true
	})
}

// mustStmt parses one statement.
func mustStmt(src string) ast.Stmt {
	f, err := parser.ParseFile(tokenFset, "", "package p; func _() {"+src+"}", 0)
	if err != nil {
		panic("dstmigrate: bad generated statement " + src + ": " + err.Error())
	}
	st := f.Decls[0].(*ast.FuncDecl).Body.List[0]
	zeroPos(st)
	return st
}
