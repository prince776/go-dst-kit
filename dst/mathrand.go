package dst

import (
	mrand "math/rand"
	"math/rand/v2"
)

// MathRand is math/rand's (v1) top-level functions for a World, by the
// same names, for code written against math/rand rather than
// math/rand/v2.
//
// In the real World each method is math/rand's own function, so Seed,
// the module's go version and the GODEBUG settings (randautoseed,
// randseednop) mean what they always have. In a simulated World they draw
// from the machine's stream "math/rand", and Seed restarts that stream
// from its argument (deterministic either way).
type MathRand interface {
	Int() int
	Intn(n int) int
	Int31() int32
	Int31n(n int32) int32
	Int63() int64
	Int63n(n int64) int64
	Uint32() uint32
	Uint64() uint64
	Float32() float32
	Float64() float64
	ExpFloat64() float64
	NormFloat64() float64
	Perm(n int) []int
	Shuffle(n int, swap func(i, j int))
	// Read is math/rand.Read: pseudo-random bytes, never an error. For
	// randomness that must be unpredictable use Rand().CryptoRead.
	Read(p []byte) (int, error)
	// Seed is math/rand.Seed.
	Seed(seed int64)
}

// MathRand is the World's math/rand (v1).
func (w *World) MathRand() MathRand {
	if w.s == nil {
		return realMathRand{}
	}
	if w.math == nil { // one simulated goroutine runs at a time
		w.math = &simMathRand{r: w.rnd.Stream("math/rand").(*simRand).Rand}
	}
	return w.math
}

type realMathRand struct{}

func (realMathRand) Int() int                           { return mrand.Int() }
func (realMathRand) Intn(n int) int                     { return mrand.Intn(n) }
func (realMathRand) Int31() int32                       { return mrand.Int31() }
func (realMathRand) Int31n(n int32) int32               { return mrand.Int31n(n) }
func (realMathRand) Int63() int64                       { return mrand.Int63() }
func (realMathRand) Int63n(n int64) int64               { return mrand.Int63n(n) }
func (realMathRand) Uint32() uint32                     { return mrand.Uint32() }
func (realMathRand) Uint64() uint64                     { return mrand.Uint64() }
func (realMathRand) Float32() float32                   { return mrand.Float32() }
func (realMathRand) Float64() float64                   { return mrand.Float64() }
func (realMathRand) ExpFloat64() float64                { return mrand.ExpFloat64() }
func (realMathRand) NormFloat64() float64               { return mrand.NormFloat64() }
func (realMathRand) Perm(n int) []int                   { return mrand.Perm(n) }
func (realMathRand) Shuffle(n int, swap func(i, j int)) { mrand.Shuffle(n, swap) }

//lint:ignore SA1019 math/rand.Read is what this stands for
func (realMathRand) Read(p []byte) (int, error) { return mrand.Read(p) }

//lint:ignore SA1019 math/rand.Seed is what this stands for
func (realMathRand) Seed(seed int64) { mrand.Seed(seed) }

// simMathRand is math/rand's API over a machine's stream; v1's panics
// for bad arguments keep their messages.
type simMathRand struct{ r *rand.Rand }

func (m *simMathRand) Int() int                           { return m.r.Int() }
func (m *simMathRand) Int31() int32                       { return m.r.Int32() }
func (m *simMathRand) Int63() int64                       { return m.r.Int64() }
func (m *simMathRand) Uint32() uint32                     { return m.r.Uint32() }
func (m *simMathRand) Uint64() uint64                     { return m.r.Uint64() }
func (m *simMathRand) Float32() float32                   { return m.r.Float32() }
func (m *simMathRand) Float64() float64                   { return m.r.Float64() }
func (m *simMathRand) ExpFloat64() float64                { return m.r.ExpFloat64() }
func (m *simMathRand) NormFloat64() float64               { return m.r.NormFloat64() }
func (m *simMathRand) Perm(n int) []int                   { return m.r.Perm(n) }
func (m *simMathRand) Shuffle(n int, swap func(i, j int)) { m.r.Shuffle(n, swap) }

func (m *simMathRand) Intn(n int) int {
	if n <= 0 {
		panic("invalid argument to Intn")
	}
	return m.r.IntN(n)
}

func (m *simMathRand) Int31n(n int32) int32 {
	if n <= 0 {
		panic("invalid argument to Int31n")
	}
	return m.r.Int32N(n)
}

func (m *simMathRand) Int63n(n int64) int64 {
	if n <= 0 {
		panic("invalid argument to Int63n")
	}
	return m.r.Int64N(n)
}

func (m *simMathRand) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i += 8 {
		v := m.r.Uint64()
		for j := 0; j < 8 && i+j < len(p); j++ {
			p[i+j] = byte(v >> (8 * j))
		}
	}
	return len(p), nil
}

// Seed restarts the stream from seed alone.
func (m *simMathRand) Seed(seed int64) {
	m.r = rand.New(rand.NewPCG(uint64(seed), uint64(seed)^0x9e3779b97f4a7c15))
}
