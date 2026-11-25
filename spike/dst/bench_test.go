package dst

import "testing"

// Ping-pong: one round trip = two channel operations across two goroutines.

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

func BenchmarkPingPongRealWorld(b *testing.B) {
	w := Real{}
	ping, pong := MakeChan[int](w, 0), MakeChan[int](w, 0)
	w.Go("echo", func() {
		for {
			v, ok := ping.Recv()
			if !ok {
				return
			}
			pong.Send(v)
		}
	})
	for i := 0; i < b.N; i++ {
		ping.Send(i)
		pong.Recv()
	}
	ping.Close()
}

func BenchmarkPingPongSim(b *testing.B) {
	Run(1, func(w World) {
		ping, pong := MakeChan[int](w, 0), MakeChan[int](w, 0)
		w.Go("echo", func() {
			for {
				v, ok := ping.Recv()
				if !ok {
					return
				}
				pong.Send(v)
			}
		})
		for i := 0; i < b.N; i++ {
			ping.Send(i)
			pong.Recv()
		}
	})
}

// Select over two channels, one always ready.

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

func BenchmarkSelectRealWorld(b *testing.B) {
	w := Real{}
	a, c := MakeChan[int](w, 1), MakeChan[int](w, 1)
	n := 0
	for i := 0; i < b.N; i++ {
		a.Send(i)
		Select(w,
			OnRecv(a, func(v int, _ bool) { n += v }),
			OnRecv(c, func(v int, _ bool) { n -= v }),
		)
	}
}
