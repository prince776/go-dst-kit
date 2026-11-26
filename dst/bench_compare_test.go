package dst_test

// Three-tier performance comparison: each operation done with bare Go
// (Raw), through the real World (Real), and in a simulated World (Sim).
// Results (Apple M1, Go 1.25) are recorded in PLAN.md.

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dsthttp"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

func inSim(b *testing.B, fn func(w *dst.World)) {
	b.Helper()
	r := simworld.Run(simworld.Options{Seed: 1, MaxSteps: 1 << 62}, fn)
	if r.Err != nil {
		b.Fatal(r.Err)
	}
}

// ---- spawn ----

func BenchmarkSpawnRaw(b *testing.B) {
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func() { wg.Done() }()
		wg.Wait()
	}
}

func spawn(w *dst.World, n int) {
	var wg dst.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		w.Go(func() { wg.Done() })
		wg.Wait()
	}
}

func BenchmarkSpawnReal(b *testing.B) { spawn(realworld.New(realworld.Options{}), b.N) }
func BenchmarkSpawnSim(b *testing.B)  { inSim(b, func(w *dst.World) { spawn(w, b.N) }) }

// ---- contended mutex: 4 goroutines ----

func BenchmarkMutexContendedRaw(b *testing.B) {
	var mu sync.Mutex
	var wg sync.WaitGroup
	n := 0
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N/4; i++ {
				mu.Lock()
				n++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
}

func contended(w *dst.World, total int) {
	var mu dst.Mutex
	var wg dst.WaitGroup
	n := 0
	for g := 0; g < 4; g++ {
		wg.Add(1)
		w.Go(func() {
			defer wg.Done()
			for i := 0; i < total/4; i++ {
				mu.Lock()
				n++
				mu.Unlock()
			}
		})
	}
	wg.Wait()
}

func BenchmarkMutexContendedReal(b *testing.B) { contended(realworld.New(realworld.Options{}), b.N) }
func BenchmarkMutexContendedSim(b *testing.B)  { inSim(b, func(w *dst.World) { contended(w, b.N) }) }

// ---- context ----

func BenchmarkContextRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
		_, cancel := context.WithTimeout(context.Background(), time.Hour)
		cancel()
	}
}

func ctxs(w *dst.World, n int) {
	for i := 0; i < n; i++ {
		_, cancel := dst.WithTimeout(w, context.Background(), time.Hour)
		cancel()
	}
}

func BenchmarkContextReal(b *testing.B) { ctxs(realworld.New(realworld.Options{}), b.N) }
func BenchmarkContextSim(b *testing.B)  { inSim(b, func(w *dst.World) { ctxs(w, b.N) }) }

// ---- timer arm + stop ----

func BenchmarkTimerRaw(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.NewTimer(time.Hour).Stop()
	}
}

func timers(w *dst.World, n int) {
	for i := 0; i < n; i++ {
		w.NewTimer(time.Hour).Stop()
	}
}

func BenchmarkTimerReal(b *testing.B) { timers(realworld.New(realworld.Options{}), b.N) }
func BenchmarkTimerSim(b *testing.B)  { inSim(b, func(w *dst.World) { timers(w, b.N) }) }

// ---- file: write 4 KiB then read it back (no fsync) ----

var page = make([]byte, 4096)

func BenchmarkFileRaw(b *testing.B) {
	f, _ := os.Create(filepath.Join(b.TempDir(), "f"))
	defer f.Close()
	buf := make([]byte, 4096)
	for i := 0; i < b.N; i++ {
		f.WriteAt(page, 0)
		f.ReadAt(buf, 0)
	}
}

func files(fsys dst.FS, n int) {
	f, err := dst.Create(fsys, "f")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	buf := make([]byte, 4096)
	for i := 0; i < n; i++ {
		f.WriteAt(page, 0)
		f.ReadAt(buf, 0)
	}
}

func BenchmarkFileReal(b *testing.B) { files(realworld.New(realworld.Options{Dir: b.TempDir()}).FS(), b.N) }
func BenchmarkFileSim(b *testing.B)  { inSim(b, func(w *dst.World) { files(w.FS(), b.N) }) }

// ---- open + close (path resolution) ----

func BenchmarkOpenRaw(b *testing.B) {
	dir := b.TempDir()
	os.WriteFile(filepath.Join(dir, "f"), page, 0o644)
	for i := 0; i < b.N; i++ {
		f, _ := os.Open(filepath.Join(dir, "f"))
		f.Close()
	}
}

func opens(fsys dst.FS, n int) {
	dst.WriteFile(fsys, "f", page, 0o644)
	for i := 0; i < n; i++ {
		f, _ := dst.Open(fsys, "f")
		f.Close()
	}
}

func BenchmarkOpenReal(b *testing.B) { opens(realworld.New(realworld.Options{Dir: b.TempDir()}).FS(), b.N) }
func BenchmarkOpenSim(b *testing.B)  { inSim(b, func(w *dst.World) { opens(w.FS(), b.N) }) }

// ---- fsync ----

func BenchmarkSyncRaw(b *testing.B) {
	f, _ := os.Create(filepath.Join(b.TempDir(), "f"))
	defer f.Close()
	for i := 0; i < b.N; i++ {
		f.WriteAt(page, 0)
		f.Sync()
	}
}

func syncs(fsys dst.FS, n int) {
	f, _ := dst.Create(fsys, "f")
	defer f.Close()
	for i := 0; i < n; i++ {
		f.WriteAt(page, 0)
		f.Sync()
	}
}

func BenchmarkSyncReal(b *testing.B) { syncs(realworld.New(realworld.Options{Dir: b.TempDir()}).FS(), b.N) }
func BenchmarkSyncSim(b *testing.B)  { inSim(b, func(w *dst.World) { syncs(w.FS(), b.N) }) }

// ---- TCP: 64-byte request/response round trip on one connection ----

func echo(ln net.Listener, spawn func(func())) {
	spawn(func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		buf := make([]byte, 64)
		for {
			if _, err := io.ReadFull(c, buf); err != nil {
				return
			}
			c.Write(buf)
		}
	})
}

func roundTrips(c net.Conn, n int) {
	buf := make([]byte, 64)
	for i := 0; i < n; i++ {
		c.Write(buf)
		io.ReadFull(c, buf)
	}
	c.Close()
}

func BenchmarkTCPRaw(b *testing.B) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	echo(ln, func(f func()) { go f() })
	c, _ := net.Dial("tcp", ln.Addr().String())
	roundTrips(c, b.N)
}

func BenchmarkTCPReal(b *testing.B) {
	w := realworld.New(realworld.Options{})
	ln, _ := w.Net().Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	echo(ln, w.Go)
	c, _ := w.Net().Dial("tcp", ln.Addr().String())
	roundTrips(c, b.N)
}

func BenchmarkTCPSim(b *testing.B) {
	inSim(b, func(w *dst.World) {
		ln, err := w.Net().Listen("tcp", "localhost:9")
		if err != nil {
			b.Fatal(err)
		}
		echo(ln, w.Go)
		c, err := w.Net().Dial("tcp", "localhost:9")
		if err != nil {
			b.Fatal(err)
		}
		roundTrips(c, b.N)
		ln.Close()
	})
}

// ---- HTTP: GET round trip ----

var okHandler = http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) { rw.Write([]byte("ok")) })

func get(c *http.Client, url string, n int) {
	for i := 0; i < n; i++ {
		resp, err := c.Get(url)
		if err != nil {
			panic(err)
		}
		io.Copy(io.Discard, bufio.NewReader(resp.Body))
		resp.Body.Close()
	}
}

func BenchmarkHTTPRaw(b *testing.B) {
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	go http.Serve(ln, okHandler)
	get(http.DefaultClient, "http://"+ln.Addr().String()+"/", b.N)
}

func BenchmarkHTTPReal(b *testing.B) {
	w := realworld.New(realworld.Options{})
	ln, _ := w.Net().Listen("tcp", "127.0.0.1:0")
	defer ln.Close()
	w.Go(func() { dsthttp.Serve(w, ln, okHandler) })
	get(dsthttp.Client(w), "http://"+ln.Addr().String()+"/", b.N)
}

func BenchmarkHTTPSim(b *testing.B) {
	inSim(b, func(w *dst.World) {
		ln, _ := w.Net().Listen("tcp", "localhost:80")
		w.Go(func() { dsthttp.Serve(w, ln, okHandler) })
		get(dsthttp.Client(w), "http://localhost:80/", b.N)
		ln.Close()
	})
}
