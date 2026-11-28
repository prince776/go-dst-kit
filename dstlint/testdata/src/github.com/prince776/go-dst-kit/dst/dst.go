// Package dst is a stub of the real one, for the analyzer's tests.
package dst

type World struct{}

func Current() *World { return nil }

func (w *World) Yield() {}

type Mutex struct{ s int }

func (m *Mutex) Lock()   {}
func (m *Mutex) Unlock() {}

type RWMutex struct{ s int }

type WaitGroup struct{ s int }

type Once struct{ m Mutex }

type Cond struct{ s int }

func NewCond(any) *Cond { return &Cond{} }

type Pool struct {
	New func() any
}

func (p *Pool) Get() any { return nil }

type Chan[T any] struct{ c chan T }

func MakeChan[T any](w *World, n int) Chan[T] { return Chan[T]{} }

type Timer struct{ C Chan[int] }

type Ticker struct{ C Chan[int] }
