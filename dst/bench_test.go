package dst_test

import (
	"testing"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

func pingPong(w *dst.World, n int) {
	ping, pong := dst.MakeChan[int](w, 0), dst.MakeChan[int](w, 0)
	w.Go(func() {
		for v := range ping.All() {
			pong.Send(v)
		}
	})
	for i := 0; i < n; i++ {
		ping.Send(i)
		pong.Recv()
	}
	ping.Close()
}

func BenchmarkPingPongRaw(b *testing.B) {
	ping, pong := make(chan int), make(chan int)
	go func() {
		for v := range ping {
			pong <- v
		}
	}()
	for i := 0; i < b.N; i++ {
		ping <- i
		<-pong
	}
	close(ping)
}

func BenchmarkPingPongReal(b *testing.B) { pingPong(realworld.New(realworld.Options{}), b.N) }

func BenchmarkPingPongSim(b *testing.B) {
	simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) { pingPong(w, b.N) })
}

func BenchmarkSelectRaw(b *testing.B) {
	a, c := make(chan int, 1), make(chan int, 1)
	n := 0
	for i := 0; i < b.N; i++ {
		a <- i
		select {
		case v := <-a:
			n += v
		case v := <-c:
			n -= v
		}
	}
}

func BenchmarkSelectReal(b *testing.B) {
	w := realworld.New(realworld.Options{})
	a, c := dst.MakeChan[int](w, 1), dst.MakeChan[int](w, 1)
	n := 0
	for i := 0; i < b.N; i++ {
		a.Send(i)
		dst.Select(w,
			dst.OnRecv(a, func(v int, _ bool) { n += v }),
			dst.OnRecv(c, func(v int, _ bool) { n -= v }),
		)
	}
}

func BenchmarkMutexUncontendedReal(b *testing.B) {
	var mu dst.Mutex
	for i := 0; i < b.N; i++ {
		mu.Lock()
		mu.Unlock()
	}
}
