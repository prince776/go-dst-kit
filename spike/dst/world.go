// Package dst is a SPIKE (throwaway): a World that runs ordinary goroutine
// code either for real or under a deterministic, seeded scheduler.
package dst

import (
	"math/rand/v2"
	"time"
)

// World is what simulated code uses instead of the language and stdlib for
// anything that waits or is nondeterministic.
type World interface {
	Go(name string, fn func()) // instead of: go fn()
	Sleep(d time.Duration)     // instead of: time.Sleep
	Now() time.Time            // instead of: time.Now
	Rand() Rand                // instead of: math/rand
}

// Rand is the World's randomness.
type Rand interface {
	IntN(n int) int
	Float64() float64
}

// Real is the production World: plain goroutines, time and randomness.
type Real struct{}

func (Real) Go(_ string, fn func()) { go fn() }
func (Real) Sleep(d time.Duration)  { time.Sleep(d) }
func (Real) Now() time.Time         { return time.Now() }
func (Real) Rand() Rand             { return globalRand{} }

type globalRand struct{}

func (globalRand) IntN(n int) int   { return rand.IntN(n) }
func (globalRand) Float64() float64 { return rand.Float64() }
