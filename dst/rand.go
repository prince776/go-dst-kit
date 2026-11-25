package dst

import (
	crand "crypto/rand"
	"math/rand/v2"
)

// Rand is the World's source of randomness: seeded per machine in a
// simulated World, the global generators in the real World (Read uses
// crypto/rand there).
type Rand interface {
	Int() int
	IntN(n int) int
	Int64() int64
	Int64N(n int64) int64
	Uint32() uint32
	Uint64() uint64
	Float64() float64
	Perm(n int) []int
	Shuffle(n int, swap func(i, j int))
	// Read fills p with random bytes, like crypto/rand.Read.
	Read(p []byte) (int, error)
}

type simRand struct{ *rand.Rand }

func (r simRand) Read(p []byte) (int, error) {
	for i := 0; i < len(p); i += 8 {
		v := r.Uint64()
		for j := 0; j < 8 && i+j < len(p); j++ {
			p[i+j] = byte(v >> (8 * j))
		}
	}
	return len(p), nil
}

type realRand struct{}

func (realRand) Int() int                           { return rand.Int() }
func (realRand) IntN(n int) int                     { return rand.IntN(n) }
func (realRand) Int64() int64                       { return rand.Int64() }
func (realRand) Int64N(n int64) int64               { return rand.Int64N(n) }
func (realRand) Uint32() uint32                     { return rand.Uint32() }
func (realRand) Uint64() uint64                     { return rand.Uint64() }
func (realRand) Float64() float64                   { return rand.Float64() }
func (realRand) Perm(n int) []int                   { return rand.Perm(n) }
func (realRand) Shuffle(n int, swap func(i, j int)) { rand.Shuffle(n, swap) }
func (realRand) Read(p []byte) (int, error)         { return crand.Read(p) }
