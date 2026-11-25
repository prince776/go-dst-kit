// Command dstlint reports code in //dst:simulated packages that escapes
// the dst World. Usage: dstlint ./...
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/prince776/go-dst-kit/dstlint"
)

func main() { singlechecker.Main(dstlint.Analyzer) }
