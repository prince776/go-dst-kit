package dsthttp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsthttp"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

func mux(w *dst.World) *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /hello/{name}", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("X-Test", "yes")
		fmt.Fprintf(rw, "hello %s", r.PathValue("name"))
	})
	m.HandleFunc("POST /echo", func(rw http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		rw.WriteHeader(http.StatusCreated)
		rw.Write([]byte(strings.ToUpper(string(b))))
	})
	m.HandleFunc("GET /slow", func(rw http.ResponseWriter, r *http.Request) {
		w.Sleep(10 * time.Second)
		rw.Write([]byte("finally"))
	})
	m.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("handler bug") })
	return m
}

// exercise serves the mux from srv and calls it from cli.
func exercise(srv, cli *dst.World) error {
	ln, err := srv.Net().Listen("tcp", "localhost:0")
	if err != nil {
		return err
	}
	defer ln.Close()
	srv.Go(func() { dsthttp.Serve(srv, ln, mux(srv)) })
	c := dsthttp.Client(cli)
	base := "http://" + ln.Addr().String()
	get := func(method, path, body string) (int, string, http.Header, error) {
		req, _ := http.NewRequest(method, base+path, strings.NewReader(body))
		resp, err := c.Do(req)
		if err != nil {
			return 0, "", nil, err
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b), resp.Header, err
	}
	checks := []struct {
		method, path, body string
		status             int
		want               string
	}{
		{"GET", "/hello/bob", "", 200, "hello bob"},
		{"POST", "/echo", "abc", 201, "ABC"},
		{"GET", "/missing", "", 404, "404 page not found\n"},
		{"HEAD", "/hello/x", "", 200, ""},
	}
	for _, ch := range checks {
		status, body, hdr, err := get(ch.method, ch.path, ch.body)
		if err != nil {
			return fmt.Errorf("%s %s: %v", ch.method, ch.path, err)
		}
		if status != ch.status || body != ch.want {
			return fmt.Errorf("%s %s: %d %q, want %d %q", ch.method, ch.path, status, body, ch.status, ch.want)
		}
		if ch.path == "/hello/bob" && hdr.Get("X-Test") != "yes" {
			return fmt.Errorf("header X-Test = %q", hdr.Get("X-Test"))
		}
	}
	return nil
}

// Verifies: [handlers-run-unchanged]
// Fails if: the same handlers and client code behave differently in the
// simulated World than on real net/http.
func TestHandlersRunUnchanged(t *testing.T) {
	w := realworld.New(realworld.Options{})
	if err := exercise(w, w); err != nil {
		t.Fatalf("real: %v", err)
	}
	for seed := uint64(1); seed <= 20; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		srv := sim.Machine("srv")
		var err error
		res := sim.Run(func(w *dst.World) { err = exercise(srv.World, w) })
		if res.Err != nil || err != nil {
			t.Fatalf("sim seed %d: %v %v", seed, res.Err, err)
		}
	}
}

// Verifies: [client-honours-context]
// Fails if: a request outlives its context's deadline or cancellation.
func TestClientHonoursContext(t *testing.T) {
	for _, tc := range []struct {
		name string
		ctx  func(w *dst.World) context.Context
		want error
		max  time.Duration
	}{
		{"deadline", func(w *dst.World) context.Context {
			ctx, _ := dst.WithTimeout(w, context.Background(), 100*time.Millisecond)
			return ctx
		}, context.DeadlineExceeded, 200 * time.Millisecond},
		{"cancel", func(w *dst.World) context.Context {
			ctx, cancel := dst.WithCancel(w, context.Background())
			w.AfterFunc(50*time.Millisecond, cancel)
			return ctx
		}, context.Canceled, 200 * time.Millisecond},
		// Negative: without a deadline the request waits for the slow server.
		{"none", func(*dst.World) context.Context { return context.Background() }, nil, 11 * time.Second},
	} {
		sim := simworld.New(simworld.Options{Seed: 1})
		srv := sim.Machine("srv")
		var err error
		var took time.Duration
		res := sim.Run(func(w *dst.World) {
			ln, _ := srv.World.Net().Listen("tcp", "srv:80")
			srv.World.Go(func() { dsthttp.Serve(srv.World, ln, mux(srv.World)) })
			start := w.Now()
			req, _ := http.NewRequestWithContext(tc.ctx(w), "GET", "http://srv/slow", nil)
			var resp *http.Response
			resp, err = dsthttp.Client(w).Do(req)
			if err == nil {
				io.ReadAll(resp.Body)
				resp.Body.Close()
			}
			took = w.Since(start)
		})
		if res.Err != nil {
			t.Fatalf("%s: %v", tc.name, res.Err)
		}
		if !errors.Is(err, tc.want) || took > tc.max || (tc.want == nil && took < 10*time.Second) {
			t.Fatalf("%s: err %v after %v, want %v within %v", tc.name, err, took, tc.want, tc.max)
		}
	}
}

// Verifies: [handler-panic-fails-run]
// Fails if: a handler panic is swallowed in simulation.
func TestHandlerPanicFailsRun(t *testing.T) {
	sim := simworld.New(simworld.Options{Seed: 1})
	srv := sim.Machine("srv")
	res := sim.Run(func(w *dst.World) {
		ln, _ := srv.World.Net().Listen("tcp", "srv:80")
		srv.World.Go(func() { dsthttp.Serve(srv.World, ln, mux(srv.World)) })
		ctx, _ := dst.WithTimeout(w, context.Background(), time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", "http://srv/boom", nil)
		dsthttp.Client(w).Do(req)
		w.Sleep(time.Second)
	})
	if res.Err == nil || !strings.Contains(res.Err.Error(), "handler bug") {
		t.Fatalf("got %v", res.Err)
	}
}
