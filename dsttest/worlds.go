package dsttest

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/prince776/go-dst-kit/dst"
)

var worldPtrType = reflect.TypeOf((*dst.World)(nil))

// The World's own machinery, which WorldsNot doesn't walk.
const (
	dstPkg       = "github.com/prince776/go-dst-kit/dst"
	internalPkgs = "github.com/prince776/go-dst-kit/internal/"
)

// WorldsNot walks v's object graph and returns the paths of the
// *dst.World values in it that aren't w. A system that threads a World
// through its structs uses it to find a part left on another World, most
// often a nil field that falls back to the real World, which the
// simulation notices only if that part waits:
//
//	if bad := dsttest.WorldsNot(db, w); len(bad) > 0 {
//		h.Failf("parts of the DB not on its World: %v", bad)
//	}
//
// The walk follows pointers, interfaces, struct fields (exported or not),
// arrays, slices and map keys and values; each pointer is visited once,
// so cycles end. It doesn't see into channels, functions (closures'
// captured variables) or the World's own types (package dst's and
// go-dst-kit's internals), and it stops 64
// elements into a slice, array or map and 64 levels deep. Paths start at
// v ("v.store.cache[3].w"); map entries show as [key] for keys of basic
// kinds, else [*].
func WorldsNot(v any, w *dst.World) []string {
	seen := map[uintptr]bool{}
	want := reflect.ValueOf(w).Pointer()
	var bad []string
	var walk func(v reflect.Value, path string, depth int)
	walk = func(v reflect.Value, path string, depth int) {
		if depth > 64 || !v.IsValid() {
			return
		}
		if v.Type() == worldPtrType {
			if v.Pointer() != want {
				bad = append(bad, path)
			}
			return
		}
		if p := v.Type().PkgPath(); p == dstPkg || strings.HasPrefix(p, internalPkgs) {
			return // the World's own machinery
		}
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() || seen[v.Pointer()] {
				return
			}
			seen[v.Pointer()] = true
			walk(v.Elem(), path, depth+1)
		case reflect.Interface:
			if !v.IsNil() {
				walk(v.Elem(), path, depth+1)
			}
		case reflect.Struct:
			for i := 0; i < v.NumField(); i++ {
				walk(v.Field(i), path+"."+v.Type().Field(i).Name, depth+1)
			}
		case reflect.Slice, reflect.Array:
			if !holds(v.Type().Elem()) {
				return
			}
			for i := 0; i < v.Len() && i < 64; i++ {
				walk(v.Index(i), fmt.Sprintf("%s[%d]", path, i), depth+1)
			}
		case reflect.Map:
			kt, vt := v.Type().Key(), v.Type().Elem()
			if !holds(kt) && !holds(vt) {
				return
			}
			it := v.MapRange()
			for n := 0; it.Next() && n < 64; n++ {
				key := "*"
				switch kt.Kind() {
				case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
					reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Bool:
					key = fmt.Sprint(it.Key())
				}
				walk(it.Key(), path+"[key "+key+"]", depth+1)
				walk(it.Value(), path+"["+key+"]", depth+1)
			}
		}
	}
	walk(reflect.ValueOf(v), "v", 0)
	return bad
}

// holds reports whether a value of type t may hold a pointer to a World.
func holds(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Interface, reflect.Struct, reflect.Array, reflect.Slice, reflect.Map:
		return true
	}
	return false
}
