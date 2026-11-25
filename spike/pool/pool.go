// Package pool is the classic goroutine worker pool, written as ordinary Go
// against a dst.World. Buggy is what people usually write; Fixed retries in
// place.
package pool

import (
	"errors"
	"fmt"
	"time"

	"spike/dst"
)

type Job struct{ ID, Attempt int }

type Result struct {
	ID, Attempts int
	Err          error
}

type Config struct {
	Workers, Jobs, QueueCap, MaxAttempts int
	FailProb                             float64
}

// process is the slow, flaky work.
func process(w dst.World, cfg Config, j Job) error {
	w.Sleep(time.Duration(1+w.Rand().IntN(20)) * time.Millisecond)
	if w.Rand().Float64() < cfg.FailProb {
		return errors.New("flaky")
	}
	return nil
}

// Buggy re-enqueues failed jobs on the shared jobs channel. Two latent bugs:
// a retry can be sent after the producer closed the channel (panic), and if
// every worker is re-enqueueing into a full channel while the producer is
// also blocked on it, nothing ever reads it again (deadlock).
func Buggy(w dst.World, cfg Config) []Result {
	jobs := dst.MakeChan[Job](w, cfg.QueueCap)
	results := dst.MakeChan[Result](w, cfg.QueueCap)
	for i := range cfg.Workers {
		w.Go(fmt.Sprintf("worker-%d", i), func() {
			for {
				j, ok := jobs.Recv()
				if !ok {
					return
				}
				err := process(w, cfg, j)
				if err != nil && j.Attempt+1 < cfg.MaxAttempts {
					j.Attempt++
					w.Sleep(10 * time.Millisecond)
					jobs.Send(j) // retry later
					continue
				}
				results.Send(Result{ID: j.ID, Attempts: j.Attempt + 1, Err: err})
			}
		})
	}
	w.Go("producer", func() {
		for i := range cfg.Jobs {
			jobs.Send(Job{ID: i})
		}
		jobs.Close()
	})
	var out []Result
	for range cfg.Jobs {
		r, _ := results.Recv()
		out = append(out, r)
	}
	return out
}

// Fixed retries inside the worker, so nothing is ever sent on jobs except
// by the producer, and collects results with a deadline using Select.
func Fixed(w dst.World, cfg Config) ([]Result, error) {
	jobs := dst.MakeChan[Job](w, cfg.QueueCap)
	results := dst.MakeChan[Result](w, cfg.QueueCap)
	for i := range cfg.Workers {
		w.Go(fmt.Sprintf("worker-%d", i), func() {
			for {
				j, ok := jobs.Recv()
				if !ok {
					return
				}
				var err error
				for {
					if err = process(w, cfg, j); err == nil || j.Attempt+1 >= cfg.MaxAttempts {
						break
					}
					j.Attempt++
					w.Sleep(10 * time.Millisecond)
				}
				results.Send(Result{ID: j.ID, Attempts: j.Attempt + 1, Err: err})
			}
		})
	}
	w.Go("producer", func() {
		for i := range cfg.Jobs {
			jobs.Send(Job{ID: i})
		}
		jobs.Close()
	})
	var out []Result
	deadline := dst.After(w, time.Minute)
	for len(out) < cfg.Jobs {
		timedOut := false
		dst.Select(w,
			dst.OnRecv(results, func(r Result, _ bool) { out = append(out, r) }),
			dst.OnRecv(deadline, func(time.Time, bool) { timedOut = true }),
		)
		if timedOut {
			return out, fmt.Errorf("timed out with %d of %d results", len(out), cfg.Jobs)
		}
	}
	return out, nil
}
