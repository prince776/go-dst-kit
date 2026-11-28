package dstlint_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/prince776/go-dst-kit/dstlint"
)

// Verifies: [flags-escapes], [respects-opt-in], [respects-ok], [flags-current]
// Fails if: an escape in an opted-in package goes unreported, a package
// without //dst:simulated is reported for anything but dst.Current, a use
// of dst.Current goes unreported in either kind of package, or //dst:ok
// doesn't suppress.
func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), dstlint.Analyzer, "sim", "plain")
}

// Verifies: [flags-unset-world], [respects-ok]
// Fails if: a literal leaving its World unset while one is at hand goes
// unreported (from a parameter, or a receiver two fields away, or with a
// method called on it that isn't a setter), or one is reported that sets
// it (in the literal, unkeyed, by a later assignment or setter, directly
// or through another setter), has no World at hand but its own variable,
// is excused with //dst:ok, or is in a _test.go file.
func TestUnsetWorld(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), dstlint.Analyzer, "unsetworld")
}

// Verifies: [flags-shared-primitive], [respects-ok]
// Fails if: a package-level variable that is or holds a waiting dst
// primitive (directly, in a var block, through NewCond, in an array or a
// nested struct) goes unreported, or one is reported that doesn't (a
// Pool, a pointer to a lock, a type's field), or //dst:ok doesn't excuse.
func TestSharedPrimitive(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), dstlint.Analyzer, "sharedprim")
}
