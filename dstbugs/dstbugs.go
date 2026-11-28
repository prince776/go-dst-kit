// Package dstbugs switches bugs planted for simulation tests, and
// candidate fixes for what they find, in the code under test.
//
// A planted bug re-inserts a known bug, to show a simulation test
// catches it; a candidate fix is tried before it replaces the code:
//
//	if dstbugs.Bug("ack-before-fsync") {
//		ack() // the bug: acknowledged before it is durable
//	}
//	f.Sync()
//	...
//	if dstbugs.Fix("deliver-unlocked") {
//		queueEvent(ev) // the fix being tried
//	} else {
//		deliver(ev) // the code as it is
//	}
//
// Only a build with the dstbugs tag can turn either on (go test -tags
// dstbugs). There, Bug and Fix report whether the name is listed in
// $DSTBUG or $DSTFIX (comma-separated), read on each call so a test can
// t.Setenv before its sweep, and count how often each switch is reached.
// In every other build both are the constant false and inline away: the
// code is the code as it is, and production pays nothing.
//
// Names are one namespace: a Bug and a Fix of the same name count
// together in Hits.
//
// Verified:
//   - [off-is-free] Without the tag, Bug and Fix are false whatever the
//     environment, Hits is 0, Tagged is false, and Bug and Fix inline.
//   - [on-reads-env] With the tag, Bug(name) is true exactly when name is
//     one of $DSTBUG's comma-separated names (Fix likewise with
//     $DSTFIX, neither seeing the other's), following a change to the
//     environment at the next call; the empty name is never on.
//   - [hits-count-reaches] With the tag, Hits(name) is the number of
//     Bug and Fix calls with that name so far, on or off, from any
//     goroutine.
package dstbugs
