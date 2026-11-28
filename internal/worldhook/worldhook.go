// Package worldhook lets the World implementations (realworld, the
// simulator) construct a *dst.World without dst exporting a constructor
// that users could misuse. dst sets NewWorld in its init function.
package worldhook

import (
	"context"
	"time"

	"github.com/prince776/go-dst-kit/internal/sched"
)

// Spec describes a World. A nil Sched means the real World.
type Spec struct {
	Sched   *sched.Scheduler
	Machine *sched.Machine
	Epoch   time.Time // simulated time zero
	FS      any       // a dst.FS, or nil
	Net     any       // a dst.Net, or nil
}

// NewWorld returns a *dst.World (as any, to avoid an import cycle).
var NewWorld func(Spec) any

// SetCurrentReal makes w (a *dst.World) the real World dst.Current
// returns. realworld's init calls it with realworld.Default(), so the
// two are one World, with the os filesystem and network.
var SetCurrentReal func(w any)

// AfterDoneSync is dst's AfterDone with f run synchronously by whoever
// ends a simulated context (the library's own network and HTTP plumbing,
// which must react within the same step); dst.AfterDone runs f in its
// own goroutine, as context.AfterFunc does. dst sets it in its init
// function.
var AfterDoneSync func(ctx context.Context, f func()) (stop func() bool)
