// Package workerpool runs a function over many inputs with a bounded
// number of goroutines, returning results in order, or the first error
// after stopping the rest. It is plain concurrent Go (goroutines,
// channels, a WaitGroup, a context) migrated to a dst.World by mechanical
// substitution:
//
//	go f()                 -> w.Go(f)
//	make(chan T, n)        -> dst.MakeChan[T](w, n)
//	ch <- v / <-ch         -> ch.Send(v) / ch.Recv()
//	for v := range ch      -> for v := range ch.All()
//	select { ... }         -> dst.Select(w, dst.OnRecv(...), dst.OnDone(ctx, ...))
//	sync.WaitGroup         -> dst.WaitGroup
//	context.WithCancel     -> dst.WithCancel
//
// Bugs plants common Go concurrency mistakes; the test shows the
// simulator catches each.
//
// Verified:
//   - [map-is-correct] Map returns every result in input order, or the
//     first error, under every interleaving the seeds explore.
//   - [no-leaks] Map returns only after every goroutine it started has
//     finished, also on error.
//
// Depends on: dst
//
//dst:simulated
package workerpool

import (
	"context"

	"github.com/prince776/go-dst-kit/dst"
)

// Bugs are mistakes Map can be told to make.
type Bugs struct {
	// AddInWorker calls wg.Add inside the worker goroutine, so Wait can
	// return before a worker has started.
	AddInWorker bool
	// ReturnOnError returns at the first error without cancelling or
	// waiting: workers block forever sending results nobody reads.
	ReturnOnError bool
	// CloseEarly closes the results channel when the producer is done
	// instead of when the workers are: a late worker sends on a closed
	// channel.
	CloseEarly bool
}

type result struct {
	i   int
	v   int
	err error
}

// Map applies fn to each input using workers goroutines.
func Map(w *dst.World, ctx context.Context, inputs []int, workers int, fn func(context.Context, int) (int, error), bugs Bugs) ([]int, error) {
	ctx, cancel := dst.WithCancel(w, ctx)
	defer cancel()
	jobs := dst.MakeChan[int](w, 0)
	results := dst.MakeChan[result](w, 0)
	var wg dst.WaitGroup
	for range workers {
		if !bugs.AddInWorker {
			wg.Add(1)
		}
		w.Go(func() {
			if bugs.AddInWorker {
				wg.Add(1)
			}
			defer wg.Done()
			for i := range jobs.All() {
				v, err := fn(ctx, inputs[i])
				results.Send(result{i, v, err})
			}
		})
	}
	w.Go(func() {
		if bugs.CloseEarly {
			defer results.Close() // runs after jobs.Close, before workers finish
		}
		defer jobs.Close()
		for i := range inputs {
			stop := false
			dst.Select(w,
				dst.OnSend(jobs, i, nil),
				dst.OnDone(ctx, func() { stop = true }),
			)
			if stop {
				return
			}
		}
	})
	if !bugs.CloseEarly {
		w.Go(func() {
			wg.Wait()
			results.Close()
		})
	}
	out := make([]int, len(inputs))
	var first error
	for r := range results.All() {
		if r.err != nil && first == nil {
			first = r.err
			if bugs.ReturnOnError {
				return nil, first
			}
			cancel() // stop the producer; workers drain what they hold
		}
		out[r.i] = r.v
	}
	if first != nil {
		return nil, first
	}
	return out, nil
}
