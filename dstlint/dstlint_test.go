package dstlint_test

import (
	"testing"

	"golang.org/x/tools/go/analysis/analysistest"

	"github.com/prince776/go-dst-kit/dstlint"
)

// Verifies: [flags-escapes], [respects-opt-in], [respects-ok]
// Fails if: an escape in an opted-in package goes unreported, a package
// without //dst:simulated is reported, or //dst:ok doesn't suppress.
func TestAnalyzer(t *testing.T) {
	analysistest.Run(t, analysistest.TestData(), dstlint.Analyzer, "sim", "plain")
}
