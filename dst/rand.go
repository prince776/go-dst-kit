package dst

import (
	crand "crypto/rand"
	"io"
	"math/rand/v2"
)

// Rand is the World's source of randomness: seeded per machine in a
// simulated World, so a seed replays; the global generators in the real
// World. The number methods stand in for math/rand; CryptoRead stands in
// for crypto/rand.Read (crypto/rand in the real World).
//
// Key generation in the crypto packages doesn't replay in a simulation
// whatever reader you pass: since Go 1.26 most crypto functions ignore the
// reader and use system randomness, and before that some read an extra
// byte at random. Give simulated machines fixed keys.
type Rand interface {
	// math/rand/v2's top-level functions, by the same names.
	Int() int
	IntN(n int) int
	Int32() int32
	Int32N(n int32) int32
	Int64() int64
	Int64N(n int64) int64
	Uint() uint
	UintN(n uint) uint
	Uint32() uint32
	Uint32N(n uint32) uint32
	Uint64() uint64
	Uint64N(n uint64) uint64
	Float32() float32
	Float64() float64
	ExpFloat64() float64
	NormFloat64() float64
	Perm(n int) []int
	Shuffle(n int, swap func(i, j int))
	// CryptoRead fills p with random bytes: crypto/rand.Read.
	CryptoRead(p []byte) (int, error)
	// Stream is an independent stream named name, derived from this one's
	// seed: drawing from it never changes what this stream (or any other)
	// draws next, so code that consumes a varying amount of randomness
	// (crypto, a retry loop, a library) can't shift the rest of the run.
	// The same name returns the same stream, continuing where it left
	// off. In the real World every stream is the global generator.
	Stream(name string) Rand
}

// CryptoReader is crypto/rand.Reader for a World, for libraries that take
// an io.Reader of random bytes. In the real World it is crypto/rand.Reader
// itself (code and crypto packages that check for the default reader see
// it).
func CryptoReader(w *World) io.Reader {
	if w.s == nil {
		return crand.Reader
	}
	return cryptoReader{w.Rand()}
}

// CryptoText is crypto/rand.Text for a World: 26 characters of the
// standard base32 alphabet, 128 random bits. In the real World it is
// crypto/rand.Text.
func CryptoText(w *World) string {
	if w.s == nil {
		return crand.Text()
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"
	src := make([]byte, 26)
	w.Rand().CryptoRead(src)
	for i := range src {
		src[i] = alphabet[src[i]%32]
	}
	return string(src)
}

type cryptoReader struct{ r Rand }

func (c cryptoReader) Read(p []byte) (int, error) { return c.r.CryptoRead(p) }

type simRand struct {
	*rand.Rand
	label   string
	derive  func(label string) *rand.Rand
	streams map[string]*simRand
}

func newSimRand(label string, derive func(string) *rand.Rand) *simRand {
	return &simRand{Rand: derive(label), label: label, derive: derive}
}

func (r *simRand) Stream(name string) Rand {
	if s, ok := r.streams[name]; ok {
		return s
	}
	if r.streams == nil {
		r.streams = map[string]*simRand{}
	}
	s := newSimRand(r.label+"/"+name, r.derive)
	r.streams[name] = s
	return s
}

func (r *simRand) CryptoRead(p []byte) (int, error) {
	for i := 0; i < len(p); i += 8 {
		v := r.Uint64()
		for j := 0; j < 8 && i+j < len(p); j++ {
			p[i+j] = byte(v >> (8 * j))
		}
	}
	return len(p), nil
}

// N is math/rand/v2's N on r: a random value in [0, n) of n's integer
// type (a time.Duration, say). It panics if n <= 0. In the real World it
// is rand.N itself.
func N[Int intType](r Rand, n Int) Int {
	if _, real := r.(realRand); real {
		return rand.N(n)
	}
	if n <= 0 {
		panic("invalid argument to N")
	}
	return Int(r.Uint64N(uint64(n)))
}

type intType interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type realRand struct{}

func (realRand) Int() int                           { return rand.Int() }
func (realRand) IntN(n int) int                     { return rand.IntN(n) }
func (realRand) Int64() int64                       { return rand.Int64() }
func (realRand) Int64N(n int64) int64               { return rand.Int64N(n) }
func (realRand) Int32() int32                       { return rand.Int32() }
func (realRand) Int32N(n int32) int32               { return rand.Int32N(n) }
func (realRand) Uint() uint                         { return rand.Uint() }
func (realRand) UintN(n uint) uint                  { return rand.UintN(n) }
func (realRand) Uint32N(n uint32) uint32            { return rand.Uint32N(n) }
func (realRand) Uint64N(n uint64) uint64            { return rand.Uint64N(n) }
func (realRand) Float32() float32                   { return rand.Float32() }
func (realRand) ExpFloat64() float64                { return rand.ExpFloat64() }
func (realRand) NormFloat64() float64               { return rand.NormFloat64() }
func (realRand) Uint32() uint32                     { return rand.Uint32() }
func (realRand) Uint64() uint64                     { return rand.Uint64() }
func (realRand) Float64() float64                   { return rand.Float64() }
func (realRand) Perm(n int) []int                   { return rand.Perm(n) }
func (realRand) Shuffle(n int, swap func(i, j int)) { rand.Shuffle(n, swap) }
func (realRand) CryptoRead(p []byte) (int, error)   { return crand.Read(p) }
func (realRand) Stream(string) Rand                 { return realRand{} }
