//dst:simulated
package pooled

import (
	"sync"

	"github.com/prince776/go-dst-kit/dst"
)

type task struct {
	done dst.Chan[error]
}

type wrapped struct{ inner struct{ t *dst.Timer } }

type plain struct{ buf []byte }

type node struct {
	next *node
	tick *dst.Ticker
}

func pools(w *dst.World) {
	_ = dst.Pool{New: func() any {
		return &task{done: dst.MakeChan[error](w, 1)} // want `a pooled value holding a dst.Chan`
	}}
	_ = &dst.Pool{New: func() any { return &wrapped{} }} // want `a pooled value holding a dst.Timer`
	var p dst.Pool
	p.New = func() any { return &node{} } // want `a pooled value holding a dst.Ticker`
	_ = dst.Pool{New: func() any { return &plain{} }}
	_ = sync.Pool{New: func() any { return &task{} }} // want `sync.Pool escapes the World` `a pooled value holding a dst.Chan`
	_ = dst.Pool{New: func() any {
		return &task{} //dst:ok remade when the World changes
	}}
}
