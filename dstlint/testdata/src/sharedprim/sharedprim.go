//dst:simulated
package sharedprim

import "github.com/prince776/go-dst-kit/dst"

var mu dst.Mutex // want `package-level dst.Mutex \(in mu\): every simulation running at once`

var (
	rw   dst.RWMutex        // want `package-level dst.RWMutex \(in rw\)`
	wg   dst.WaitGroup      // want `package-level dst.WaitGroup \(in wg\)`
	once dst.Once           // want `package-level dst.Once \(in once\)`
	cond = dst.NewCond(&mu) // want `package-level dst.Cond \(in cond\)`
	cv   dst.Cond           // want `package-level dst.Cond \(in cv\)`
	lks  [4]dst.Mutex       // want `package-level dst.Mutex \(in lks\)`
)

// registry holds a lock: the common global-cache shape.
var registry struct { // want `package-level dst.Mutex \(in registry\)`
	mu sync
	m  map[string]int
}

type sync struct{ inner struct{ l dst.Mutex } }

var excused dst.Mutex //dst:ok no seed runs in parallel with another here

// Not reported: values goroutines don't wait on, pointers to locks (the
// pointee is someone's instance), and a Pool.
var (
	pool  dst.Pool
	ptr   *dst.Mutex
	count int
)

type server struct{ mu dst.Mutex } // a type: each instance has its own

func newServer() *server { return &server{} }
