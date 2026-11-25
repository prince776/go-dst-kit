// Package dsthttp runs net/http code in a World: your http.Handlers and
// http.Client calls stay unchanged; only how the server is started and
// which transport the client uses come from here.
//
//	go dsthttp.Serve(w, ln, mux)                          // instead of http.Serve(ln, mux)
//	client := &http.Client{Transport: dsthttp.Transport(w)} // instead of http.DefaultClient
//
// In the real World these are http.Serve and http.DefaultTransport. In a
// simulated World, the server is a small loop that parses requests with
// http.ReadRequest and calls the handler, and the transport dials through
// the World's network, one connection per request.
//
// In simulated code, bound requests with a context (dst.WithTimeout), not
// http.Client.Timeout, which uses real timers.
//
// Verified:
//   - [handlers-run-unchanged] The same handlers and client calls work in
//     both Worlds (status, headers, bodies, keep-alive, methods).
//   - [client-honours-context] A request ends with the context's error once
//     its deadline passes or it is cancelled, even if the server never
//     answers.
//   - [handler-panic-fails-run] A panic in a handler fails the simulated
//     run (unlike net/http, which logs it), except http.ErrAbortHandler.
//
// Depends on: dst
package dsthttp

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/prince776/go-dst-kit/dst"
)

// Serve accepts connections on ln and serves each with h, until ln fails.
func Serve(w *dst.World, ln net.Listener, h http.Handler) error {
	if !w.Simulated() {
		return http.Serve(ln, h)
	}
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		w.GoNamed("http "+c.RemoteAddr().String(), func() { serveConn(w, c, h) })
	}
}

func serveConn(w *dst.World, c net.Conn, h http.Handler) {
	defer c.Close()
	br := bufio.NewReader(c)
	for {
		req, err := http.ReadRequest(br)
		if err != nil {
			return
		}
		req.RemoteAddr = c.RemoteAddr().String()
		ctx, cancel := dst.WithCancel(w, context.Background())
		req = req.WithContext(ctx)
		rw := &responseWriter{header: http.Header{}, req: req}
		aborted := serveOne(h, rw, req)
		cancel()
		io.Copy(io.Discard, req.Body)
		req.Body.Close()
		if aborted {
			return
		}
		resp := rw.response()
		if err := resp.Write(c); err != nil || req.Close || resp.Close {
			return
		}
	}
}

// serveOne runs the handler, reporting whether it aborted with
// http.ErrAbortHandler. Any other panic propagates and fails the run.
func serveOne(h http.Handler, rw http.ResponseWriter, req *http.Request) (aborted bool) {
	defer func() {
		if r := recover(); r != nil {
			if r == http.ErrAbortHandler {
				aborted = true
				return
			}
			panic(fmt.Sprintf("http handler for %s %s panicked: %v", req.Method, req.URL, r))
		}
	}()
	h.ServeHTTP(rw, req)
	return false
}

type responseWriter struct {
	header http.Header
	req    *http.Request
	status int
	body   bytes.Buffer
}

func (r *responseWriter) Header() http.Header { return r.header }

func (r *responseWriter) WriteHeader(status int) {
	if r.status == 0 {
		r.status = status
	}
}

func (r *responseWriter) Write(p []byte) (int, error) {
	r.WriteHeader(http.StatusOK)
	return r.body.Write(p)
}

// Flush is a no-op: the response is sent when the handler returns.
func (r *responseWriter) Flush() {}

func (r *responseWriter) response() *http.Response {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if r.header.Get("Content-Type") == "" && r.body.Len() > 0 {
		r.header.Set("Content-Type", http.DetectContentType(r.body.Bytes()))
	}
	return &http.Response{
		StatusCode:    r.status,
		ProtoMajor:    1,
		ProtoMinor:    1,
		Header:        r.header,
		Body:          io.NopCloser(bytes.NewReader(r.body.Bytes())),
		ContentLength: int64(r.body.Len()),
		Request:       r.req,
		Close:         r.req.Close,
	}
}

// Transport is the http.RoundTripper for w.
func Transport(w *dst.World) http.RoundTripper {
	if !w.Simulated() {
		return http.DefaultTransport
	}
	return transport{w}
}

// Client returns an http.Client using Transport(w).
func Client(w *dst.World) *http.Client { return &http.Client{Transport: Transport(w)} }

type transport struct{ w *dst.World }

var longAgo = time.Unix(1, 0)

func (t transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Scheme != "http" {
		return nil, fmt.Errorf("dsthttp: unsupported scheme %q", req.URL.Scheme)
	}
	host := req.URL.Host
	if !strings.Contains(host, ":") {
		host += ":80"
	}
	ctx := req.Context()
	c, err := t.w.Net().DialContext(ctx, "tcp", host)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	if dl, ok := ctx.Deadline(); ok {
		c.SetDeadline(dl)
	}
	stop := dst.AfterDone(ctx, func() { c.SetDeadline(longAgo) })
	fail := func(err error) (*http.Response, error) {
		stop()
		c.Close()
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, err
	}
	out := req.Clone(ctx)
	out.Close = true // one request per connection
	if err := out.Write(c); err != nil {
		return fail(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(c), req)
	if err != nil {
		return fail(err)
	}
	resp.Body = &body{ReadCloser: resp.Body, c: c, stop: stop, ctx: ctx}
	return resp, nil
}

type body struct {
	io.ReadCloser
	c    net.Conn
	stop func() bool
	ctx  context.Context
}

func (b *body) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && !errors.Is(err, io.EOF) && b.ctx.Err() != nil {
		err = b.ctx.Err()
	}
	return n, err
}

func (b *body) Close() error {
	b.stop()
	b.ReadCloser.Close()
	return b.c.Close()
}
