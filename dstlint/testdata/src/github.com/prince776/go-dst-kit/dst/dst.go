// Package dst is a stub of the real one, for the analyzer's tests.
package dst

type World struct{}

func Current() *World { return nil }

func (w *World) Yield() {}
