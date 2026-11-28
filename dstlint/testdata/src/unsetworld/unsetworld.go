//dst:simulated
package unsetworld

import "github.com/prince776/go-dst-kit/dst"

// Store has a World; left unset, it falls back to the real one.
type Store struct {
	w    *dst.World
	name string
}

// setWorld sets s's World: a setter.
func (s *Store) setWorld(w *dst.World) { s.w = w }

// init2 sets it through a setter: also a setter.
func (s *Store) init2(w *dst.World) { s.setWorld(w) }

// rename doesn't touch the World: not a setter.
func (s *Store) rename(n string) { s.name = n }

type Env struct{ W *dst.World }

type Outer struct{ env Env }

// A World as a parameter.
func fromParam(w *dst.World) *Store {
	return &Store{name: "a"} // want `Store literal leaves w unset while w is at hand`
}

// A World two selections away, through the receiver.
func (o *Outer) fromReceiver() Store {
	return Store{name: "b"} // want `Store literal leaves w unset while o.env.W is at hand`
}

// Set in the literal.
func setInLiteral(w *dst.World) *Store { return &Store{w: w, name: "c"} }

// Unkeyed: every field set.
func unkeyed(w *dst.World) Store { return Store{w, "d"} }

// No World at hand.
func noWorld() *Store { return &Store{name: "e"} }

// Set by assignment afterwards.
func assignedLater(w *dst.World) *Store {
	s := &Store{name: "f"}
	s.w = w
	return s
}

// Set by a setter afterwards, directly or through another setter.
func setterLater(w *dst.World) *Store {
	s := &Store{name: "g"}
	s.setWorld(w)
	t := &Store{name: "h"}
	t.init2(w)
	return s
}

// A method that isn't a setter doesn't count.
func notASetter(w *dst.World) *Store {
	s := &Store{name: "i"} // want `Store literal leaves w unset while w is at hand`
	s.rename("j")
	return s
}

// The literal's own variable isn't a World at hand: it is what is being
// set up.
func ownVariable() {
	var s Store
	s = Store{name: "k"}
	_ = s
}

// Nested in a literal whose variable reaches a World: that variable is
// being set up too, so it isn't a World at hand.
type Sched struct {
	world *dst.World
	ts    Store
}

func nestedInOwn() *Sched {
	s := &Sched{ts: Store{name: "m"}}
	return s
}

// Excused.
func excused(w *dst.World) *Store {
	return &Store{name: "l"} //dst:ok a probe that never runs in a simulation
}
