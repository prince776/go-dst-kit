package plain

import (
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

func f() {
	go f()
	time.Sleep(time.Second)
	_ = dst.Current() // want `dst.Current is a last resort`
	_ = dst.Current() //dst:ok no World reaches this leaf
}
