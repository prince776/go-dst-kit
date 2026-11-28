package dst

import (
	"context"
	"io"
	"net"
	"os"
	"time"
)

// Pipe is io.Pipe for a World: a synchronous in-memory pipe, each Write
// waiting until Reads have taken all of it. In the real World its ends
// are io.Pipe's; in a simulated World they wait through the World (io's
// own pipe would block outside the scheduler), following io's algorithm
// step for step, so reads, writes, closes and their errors are io's.
func Pipe(w *World) (*PipeReader, *PipeWriter) {
	if w.s == nil {
		r, wr := io.Pipe()
		e := &pipeEnds{PipeReader{real: r}, PipeWriter{real: wr}} // one allocation for both
		return &e.r, &e.w
	}
	p := &pipe{
		w:    w,
		wrCh: MakeChan[[]byte](w, 0),
		rdCh: MakeChan[int](w, 0),
		done: MakeChan[struct{}](w, 0),
	}
	return &PipeReader{p: p}, &PipeWriter{p: p}
}

// pipeEnds holds a real Pipe's two ends.
type pipeEnds struct {
	r PipeReader
	w PipeWriter
}

// PipeReader is io.PipeReader for a World: the read half of a Pipe.
type PipeReader struct {
	real *io.PipeReader
	p    *pipe
}

// Read is (*io.PipeReader).Read.
func (r *PipeReader) Read(data []byte) (int, error) {
	if r.real != nil {
		return r.real.Read(data)
	}
	return r.p.read(data)
}

// Close is (*io.PipeReader).Close.
func (r *PipeReader) Close() error { return r.CloseWithError(nil) }

// CloseWithError is (*io.PipeReader).CloseWithError.
func (r *PipeReader) CloseWithError(err error) error {
	if r.real != nil {
		return r.real.CloseWithError(err)
	}
	return r.p.closeRead(err)
}

// PipeWriter is io.PipeWriter for a World: the write half of a Pipe.
type PipeWriter struct {
	real *io.PipeWriter
	p    *pipe
}

// Write is (*io.PipeWriter).Write.
func (w *PipeWriter) Write(data []byte) (int, error) {
	if w.real != nil {
		return w.real.Write(data)
	}
	return w.p.write(data)
}

// Close is (*io.PipeWriter).Close.
func (w *PipeWriter) Close() error { return w.CloseWithError(nil) }

// CloseWithError is (*io.PipeWriter).CloseWithError.
func (w *PipeWriter) CloseWithError(err error) error {
	if w.real != nil {
		return w.real.CloseWithError(err)
	}
	return w.p.closeWrite(err)
}

// pipe is io's pipe (io/pipe.go) over the World's channels and locks.
type pipe struct {
	w    *World
	wrMu Mutex // serializes writes
	wrCh Chan[[]byte]
	rdCh Chan[int]

	once Once // protects closing done
	done Chan[struct{}]
	rerr onceError
	werr onceError
}

// onceError is an error that keeps the first one stored.
type onceError struct {
	Mutex
	err error
}

func (a *onceError) Store(err error) {
	a.Lock()
	defer a.Unlock()
	if a.err != nil {
		return
	}
	a.err = err
}

func (a *onceError) Load() error {
	a.Lock()
	defer a.Unlock()
	return a.err
}

// closed reports whether c, a channel that is only ever closed, is.
func closed(c Chan[struct{}]) bool {
	_, ok, received := c.TryRecv()
	return received && !ok
}

func (p *pipe) read(b []byte) (n int, err error) {
	if closed(p.done) {
		return 0, p.readCloseError()
	}
	Select2(p.w,
		OnRecv(p.wrCh, func(bw []byte, _ bool) {
			n = copy(b, bw)
			p.rdCh.Send(n)
		}),
		OnRecv(p.done, func(struct{}, bool) { err = p.readCloseError() }),
	)
	return n, err
}

func (p *pipe) closeRead(err error) error {
	if err == nil {
		err = io.ErrClosedPipe
	}
	p.rerr.Store(err)
	p.once.Do(p.done.Close)
	return nil
}

func (p *pipe) write(b []byte) (n int, err error) {
	if closed(p.done) {
		return 0, p.writeCloseError()
	}
	p.wrMu.Lock()
	defer p.wrMu.Unlock()
	for once := true; once || len(b) > 0; once = false {
		stopped := false
		Select2(p.w,
			OnSend(p.wrCh, b, func() {
				nw, _ := p.rdCh.Recv()
				b = b[nw:]
				n += nw
			}),
			OnRecv(p.done, func(struct{}, bool) { err, stopped = p.writeCloseError(), true }),
		)
		if stopped {
			return n, err
		}
	}
	return n, nil
}

func (p *pipe) closeWrite(err error) error {
	if err == nil {
		err = io.EOF
	}
	p.werr.Store(err)
	p.once.Do(p.done.Close)
	return nil
}

func (p *pipe) readCloseError() error {
	rerr := p.rerr.Load()
	if werr := p.werr.Load(); rerr == nil && werr != nil {
		return werr
	}
	return io.ErrClosedPipe
}

func (p *pipe) writeCloseError() error {
	werr := p.werr.Load()
	if rerr := p.rerr.Load(); werr == nil && rerr != nil {
		return rerr
	}
	return io.ErrClosedPipe
}

// DialTimeout is net.DialTimeout for a World: a Dial through w's network
// that gives up after timeout (by the World's clock). In the real World it
// is net.DialTimeout.
func DialTimeout(w *World, network, address string, timeout time.Duration) (net.Conn, error) {
	if w.s == nil {
		return net.DialTimeout(network, address, timeout)
	}
	ctx, cancel := WithTimeout(w, context.Background(), timeout)
	defer cancel()
	return w.Net().DialContext(ctx, network, address)
}

// NetPipe is net.Pipe for a World: a synchronous, in-memory, full-duplex
// connection, with deadlines. In the real World it is net.Pipe; in a
// simulated World it follows net's algorithm (net/pipe.go) over the
// World's channels, locks and timers, so deadlines follow the World's
// clock.
func NetPipe(w *World) (net.Conn, net.Conn) {
	if w.s == nil {
		return net.Pipe()
	}
	cb1, cb2 := MakeChan[[]byte](w, 0), MakeChan[[]byte](w, 0)
	cn1, cn2 := MakeChan[int](w, 0), MakeChan[int](w, 0)
	done1, done2 := MakeChan[struct{}](w, 0), MakeChan[struct{}](w, 0)
	p1 := &netPipe{w: w, rdRx: cb1, rdTx: cn1, wrTx: cb2, wrRx: cn2, localDone: done1, remoteDone: done2,
		readDeadline: makePipeDeadline(w), writeDeadline: makePipeDeadline(w)}
	p2 := &netPipe{w: w, rdRx: cb2, rdTx: cn2, wrTx: cb1, wrRx: cn1, localDone: done2, remoteDone: done1,
		readDeadline: makePipeDeadline(w), writeDeadline: makePipeDeadline(w)}
	return p1, p2
}

type pipeDeadline struct {
	w      *World
	mu     Mutex // guards timer and cancel
	timer  *Timer
	cancel Chan[struct{}] // closed when the deadline passes
}

func makePipeDeadline(w *World) *pipeDeadline {
	return &pipeDeadline{w: w, cancel: MakeChan[struct{}](w, 0)}
}

// set is net's pipeDeadline.set: a zero t means no deadline, a past one
// times out at once, a future one at t (by the World's clock).
func (d *pipeDeadline) set(t time.Time) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.timer != nil && !d.timer.Stop() {
		d.cancel.Recv() // wait for the timer's function to close cancel
	}
	d.timer = nil
	wasClosed := closed(d.cancel)
	if t.IsZero() {
		if wasClosed {
			d.cancel = MakeChan[struct{}](d.w, 0)
		}
		return
	}
	if dur := d.w.Until(t); dur > 0 {
		if wasClosed {
			d.cancel = MakeChan[struct{}](d.w, 0)
		}
		cancel := d.cancel
		d.timer = d.w.AfterFunc(dur, cancel.Close)
		return
	}
	if !wasClosed {
		d.cancel.Close()
	}
}

func (d *pipeDeadline) wait() Chan[struct{}] {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.cancel
}

type pipeAddr struct{}

func (pipeAddr) Network() string { return "pipe" }
func (pipeAddr) String() string  { return "pipe" }

// netPipe is one end of a simulated NetPipe: net's pipe over the World.
type netPipe struct {
	w    *World
	wrMu Mutex // serializes writes

	// rdRx is received from, then rdTx sent to, by a local Read.
	rdRx Chan[[]byte]
	rdTx Chan[int]
	// wrTx is sent to, then wrRx received from, by a local Write.
	wrTx Chan[[]byte]
	wrRx Chan[int]

	once       Once // protects closing localDone
	localDone  Chan[struct{}]
	remoteDone Chan[struct{}]

	readDeadline  *pipeDeadline
	writeDeadline *pipeDeadline
}

func (*netPipe) LocalAddr() net.Addr  { return pipeAddr{} }
func (*netPipe) RemoteAddr() net.Addr { return pipeAddr{} }

func (p *netPipe) Read(b []byte) (int, error) {
	n, err := p.read(b)
	if err != nil && err != io.EOF && err != io.ErrClosedPipe {
		err = &net.OpError{Op: "read", Net: "pipe", Err: err}
	}
	return n, err
}

func (p *netPipe) read(b []byte) (n int, err error) {
	switch {
	case closed(p.localDone):
		return 0, io.ErrClosedPipe
	case closed(p.remoteDone):
		return 0, io.EOF
	case closed(p.readDeadline.wait()):
		return 0, os.ErrDeadlineExceeded
	}
	Select(p.w,
		OnRecv(p.rdRx, func(bw []byte, _ bool) {
			n = copy(b, bw)
			p.rdTx.Send(n)
		}),
		OnRecv(p.localDone, func(struct{}, bool) { err = io.ErrClosedPipe }),
		OnRecv(p.remoteDone, func(struct{}, bool) { err = io.EOF }),
		OnRecv(p.readDeadline.wait(), func(struct{}, bool) { err = os.ErrDeadlineExceeded }),
	)
	return n, err
}

func (p *netPipe) Write(b []byte) (int, error) {
	n, err := p.write(b)
	if err != nil && err != io.ErrClosedPipe {
		err = &net.OpError{Op: "write", Net: "pipe", Err: err}
	}
	return n, err
}

func (p *netPipe) write(b []byte) (n int, err error) {
	switch {
	case closed(p.localDone):
		return 0, io.ErrClosedPipe
	case closed(p.remoteDone):
		return 0, io.ErrClosedPipe
	case closed(p.writeDeadline.wait()):
		return 0, os.ErrDeadlineExceeded
	}
	p.wrMu.Lock() // the whole of b is written together
	defer p.wrMu.Unlock()
	for once := true; once || len(b) > 0; once = false {
		var stop error
		Select(p.w,
			OnSend(p.wrTx, b, func() {
				nw, _ := p.wrRx.Recv()
				b = b[nw:]
				n += nw
			}),
			OnRecv(p.localDone, func(struct{}, bool) { stop = io.ErrClosedPipe }),
			OnRecv(p.remoteDone, func(struct{}, bool) { stop = io.ErrClosedPipe }),
			OnRecv(p.writeDeadline.wait(), func(struct{}, bool) { stop = os.ErrDeadlineExceeded }),
		)
		if stop != nil {
			return n, stop
		}
	}
	return n, nil
}

func (p *netPipe) SetDeadline(t time.Time) error {
	if closed(p.localDone) || closed(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.readDeadline.set(t)
	p.writeDeadline.set(t)
	return nil
}

func (p *netPipe) SetReadDeadline(t time.Time) error {
	if closed(p.localDone) || closed(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.readDeadline.set(t)
	return nil
}

func (p *netPipe) SetWriteDeadline(t time.Time) error {
	if closed(p.localDone) || closed(p.remoteDone) {
		return io.ErrClosedPipe
	}
	p.writeDeadline.set(t)
	return nil
}

func (p *netPipe) Close() error {
	p.once.Do(p.localDone.Close)
	return nil
}
