//dst:simulated
package sim

import (
	"context"
	"math/rand/v2"
	"os"
	"sync"
	"time"
)

var mu sync.Mutex // want `sync.Mutex escapes the World; use dst.Mutex`

func f(ctx context.Context) {
	go f(ctx)                  // want `go statement escapes the World; use w.Go`
	c := make(chan int, 1)     // want `make\(chan\) escapes the World; use dst.MakeChan`
	c <- 1                     // want `channel send escapes the World`
	<-c                        // want `channel receive escapes the World`
	for range c {              // want `range over a channel escapes the World`
	}
	select {}                  // want `select statement escapes the World; use dst.Select`
	_ = time.Now()             // want `time.Now escapes the World; use w.Now\(\)`
	time.Sleep(time.Second)    // want `time.Sleep escapes the World; use w.Sleep`
	_ = rand.IntN(3)           // want `math/rand/v2.IntN escapes the World; use w.Rand\(\)`
	os.ReadFile("x")           // want `os.ReadFile escapes the World`
	context.WithTimeout(ctx, 1) // want `context.WithTimeout escapes the World; use dst.WithTimeout`
	_ = time.Second.String()   // methods and constants are fine
	_ = context.WithValue(ctx, 1, 2)
	mu.Lock() //dst:ok checked by hand
	//dst:ok the line below is fine too
	time.Sleep(0)
}
