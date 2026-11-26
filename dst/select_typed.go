package dst

// Select2 is Select with exactly two cases (OnRecv, OnSend or OnDone).
// In the real World it is a native select statement, about 4x cheaper
// than Select (which uses reflection); in a simulated World the two are
// the same. For a select with a default, use Select or TryRecv/TrySend.
func Select2[A, B any](w *World, a Op[A], b Op[B]) {
	if w.s != nil {
		Select(w, a, b)
		return
	}
	ac, as, av := a.c, a.send, a.v
	bc, bs, bv := b.c, b.send, b.v
	ra, sa := realChans(ac, as)
	rb, sb := realChans(bc, bs)
	switch {
	case !as && !bs:
		select {
		case v, ok := <-ra:
			a.fire(v, ok)
		case v, ok := <-rb:
			b.fire(v, ok)
		}
	case !as && bs:
		select {
		case v, ok := <-ra:
			a.fire(v, ok)
		case sb <- bv:
			b.fire(bv, true)
		}
	case as && !bs:
		select {
		case sa <- av:
			a.fire(av, true)
		case v, ok := <-rb:
			b.fire(v, ok)
		}
	default:
		select {
		case sa <- av:
			a.fire(av, true)
		case sb <- bv:
			b.fire(bv, true)
		}
	}
}

// Select3 is Select with exactly three cases; see Select2.
func Select3[A, B, C any](w *World, a Op[A], b Op[B], c Op[C]) {
	if w.s != nil {
		Select(w, a, b, c)
		return
	}
	ac, as, av := a.c, a.send, a.v
	bc, bs, bv := b.c, b.send, b.v
	cc, cs, cv := c.c, c.send, c.v
	ra, sa := realChans(ac, as)
	rb, sb := realChans(bc, bs)
	rc, sc := realChans(cc, cs)
	// One native select per combination of case directions (r = receive,
	// s = send), in the order a, b, c.
	switch {
	case !as && !bs && !cs: // r r r
		select {
		case v, ok := <-ra:
			a.fire(v, ok)
		case v, ok := <-rb:
			b.fire(v, ok)
		case v, ok := <-rc:
			c.fire(v, ok)
		}
	case !as && !bs && cs: // r r s
		select {
		case v, ok := <-ra:
			a.fire(v, ok)
		case v, ok := <-rb:
			b.fire(v, ok)
		case sc <- cv:
			c.fire(cv, true)
		}
	case !as && bs && !cs: // r s r
		select {
		case v, ok := <-ra:
			a.fire(v, ok)
		case sb <- bv:
			b.fire(bv, true)
		case v, ok := <-rc:
			c.fire(v, ok)
		}
	case !as && bs && cs: // r s s
		select {
		case v, ok := <-ra:
			a.fire(v, ok)
		case sb <- bv:
			b.fire(bv, true)
		case sc <- cv:
			c.fire(cv, true)
		}
	case as && !bs && !cs: // s r r
		select {
		case sa <- av:
			a.fire(av, true)
		case v, ok := <-rb:
			b.fire(v, ok)
		case v, ok := <-rc:
			c.fire(v, ok)
		}
	case as && !bs && cs: // s r s
		select {
		case sa <- av:
			a.fire(av, true)
		case v, ok := <-rb:
			b.fire(v, ok)
		case sc <- cv:
			c.fire(cv, true)
		}
	case as && bs && !cs: // s s r
		select {
		case sa <- av:
			a.fire(av, true)
		case sb <- bv:
			b.fire(bv, true)
		case v, ok := <-rc:
			c.fire(v, ok)
		}
	default: // s s s
		select {
		case sa <- av:
			a.fire(av, true)
		case sb <- bv:
			b.fire(bv, true)
		case sc <- cv:
			c.fire(cv, true)
		}
	}
}

// realChans returns the real channel to receive from, or to send on, for
// a case on c (the other is nil, so it is never ready).
func realChans[T any](c Chan[T], send bool) (<-chan T, chan<- T) {
	if c.sim != nil {
		panic("dst: simulated channel used in the real World")
	}
	if !send {
		return c.ro, nil
	}
	if c.rw == nil && c.ro != nil {
		panic("dst: send on a receive-only channel")
	}
	return nil, c.rw
}
