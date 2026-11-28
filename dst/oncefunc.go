package dst

// OnceFunc is sync.OnceFunc for code that runs in a World: the function
// it returns calls f only once, and callers that come while f runs wait
// for it (through the World in a simulation, as Once's do). If f panics,
// every call panics with the same value. It is sync.OnceFunc's own
// algorithm over a dst.Once, so a function made outside a simulation
// (a package-level variable) works inside one.
func OnceFunc(f func()) func() {
	d := struct {
		f     func()
		once  Once
		valid bool
		p     any
	}{f: f}
	return func() {
		d.once.Do(func() {
			defer func() {
				d.f = nil // don't keep f alive after calling it
				d.p = recover()
				if !d.valid {
					panic(d.p) // the first caller gets f's whole stack
				}
			}()
			d.f()
			d.valid = true
		})
		if !d.valid {
			panic(d.p)
		}
	}
}

// OnceValue is sync.OnceValue for code that runs in a World; see OnceFunc.
func OnceValue[T any](f func() T) func() T {
	d := struct {
		f      func() T
		once   Once
		valid  bool
		p      any
		result T
	}{f: f}
	return func() T {
		d.once.Do(func() {
			defer func() {
				d.f = nil
				d.p = recover()
				if !d.valid {
					panic(d.p)
				}
			}()
			d.result = d.f()
			d.valid = true
		})
		if !d.valid {
			panic(d.p)
		}
		return d.result
	}
}

// OnceValues is sync.OnceValues for code that runs in a World; see
// OnceFunc.
func OnceValues[T1, T2 any](f func() (T1, T2)) func() (T1, T2) {
	d := struct {
		f     func() (T1, T2)
		once  Once
		valid bool
		p     any
		r1    T1
		r2    T2
	}{f: f}
	return func() (T1, T2) {
		d.once.Do(func() {
			defer func() {
				d.f = nil
				d.p = recover()
				if !d.valid {
					panic(d.p)
				}
			}()
			d.r1, d.r2 = d.f()
			d.valid = true
		})
		if !d.valid {
			panic(d.p)
		}
		return d.r1, d.r2
	}
}
