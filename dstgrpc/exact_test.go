package dstgrpc_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dstgrpc"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

// trace records the order interceptors run in.
type trace struct {
	mu  sync.Mutex
	got []string
}

func (t *trace) add(s string) { t.mu.Lock(); t.got = append(t.got, s); t.mu.Unlock() }

func (t *trace) String() string { t.mu.Lock(); defer t.mu.Unlock(); return strings.Join(t.got, " ") }

func (t *trace) su(name string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		t.add(name)
		return h(ctx, req)
	}
}

func (t *trace) ss(name string) grpc.StreamServerInterceptor {
	return func(srv any, s grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		t.add(name)
		return h(srv, s)
	}
}

func (t *trace) cu(name string) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		t.add(name)
		return inv(ctx, method, req, reply, cc, opts...)
	}
}

func (t *trace) cs(name string) grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, st grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		t.add(name)
		return st(ctx, desc, cc, method, opts...)
	}
}

// interceptorRun serves health with every kind of interceptor option on
// srv, calls Check and Watch from cli, and returns the order they ran
// in. mk chooses how the server and client are made.
func interceptorRun(srv, cli *dst.World, addr string, exact bool) (string, error) {
	var tr trace
	sopts := []grpc.ServerOption{grpc.UnaryInterceptor(tr.su("su")), grpc.ChainUnaryInterceptor(tr.su("sc1"), tr.su("sc2")),
		grpc.ChainStreamInterceptor(tr.ss("ssc")), grpc.StreamInterceptor(tr.ss("ss"))}
	copts := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithChainUnaryInterceptor(tr.cu("cc1")), grpc.WithUnaryInterceptor(tr.cu("replaced")), grpc.WithUnaryInterceptor(tr.cu("cu")),
		grpc.WithChainUnaryInterceptor(tr.cu("cc2")), grpc.WithStreamInterceptor(tr.cs("cs")), grpc.WithChainStreamInterceptor(tr.cs("csc"))}
	if exact {
		sopts = []grpc.ServerOption{dstgrpc.UnaryInterceptorOption(tr.su("su")), dstgrpc.ChainUnaryInterceptorOption(tr.su("sc1"), tr.su("sc2")),
			dstgrpc.ChainStreamInterceptorOption(tr.ss("ssc")), dstgrpc.StreamInterceptorOption(tr.ss("ss"))}
		copts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()),
			dstgrpc.WithChainUnaryInterceptorOption(tr.cu("cc1")), dstgrpc.WithUnaryInterceptorOption(tr.cu("replaced")), dstgrpc.WithUnaryInterceptorOption(tr.cu("cu")),
			dstgrpc.WithChainUnaryInterceptorOption(tr.cu("cc2")), dstgrpc.WithStreamInterceptorOption(tr.cs("cs")), dstgrpc.WithChainStreamInterceptorOption(tr.cs("csc"))}
	}
	ln, err := srv.Net().Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	var conn grpc.ClientConnInterface
	var stop func()
	if exact {
		s := dstgrpc.NewServerExact(srv, sopts...)
		hv1.RegisterHealthServer(s, &health{})
		srv.Go(func() { s.Serve(ln) })
		stop = s.Stop
		c, err := dstgrpc.NewClientExact(cli, ln.Addr().String(), copts...)
		if err != nil {
			return "", err
		}
		defer c.Close()
		conn = c
	} else {
		s := grpc.NewServer(sopts...)
		hv1.RegisterHealthServer(s, &health{})
		go s.Serve(ln)
		stop = s.Stop
		c, err := grpc.NewClient(ln.Addr().String(), copts...)
		if err != nil {
			return "", err
		}
		defer c.Close()
		conn = c
	}
	defer stop()
	hc := hv1.NewHealthClient(conn)
	if _, err := hc.Check(context.Background(), &hv1.HealthCheckRequest{}); err != nil {
		return "", err
	}
	tr.add("|")
	st, err := hc.Watch(context.Background(), &hv1.HealthCheckRequest{})
	if err != nil {
		return "", err
	}
	for {
		if _, err := st.Recv(); err != nil {
			break
		}
	}
	return tr.String(), nil
}

// Verifies: [exact-is-grpc]
// NewServerExact and NewClientExact are grpc's: in the real World the
// client is the *grpc.ClientConn itself, and in both Worlds the carried
// interceptors run in exactly grpc-go's order (the single interceptor
// before the chain, a later WithUnaryInterceptor replacing an earlier
// one), on calls and streams, on both sides.
// Fails if an order, a replacement or a side differs from grpc-go's
// own, or the real client isn't grpc's.
func TestExactInterceptorsAreGRPCs(t *testing.T) {
	w := realworld.New(realworld.Options{})
	want, err := interceptorRun(w, w, "127.0.0.1:0", false)
	if err != nil {
		t.Fatalf("grpc-go: %v", err)
	}
	if !strings.Contains(want, "cu cc1 cc2") || strings.Contains(want, "replaced") {
		t.Fatalf("grpc-go's own order is %q", want)
	}
	if got, err := interceptorRun(w, w, "127.0.0.1:0", true); err != nil || got != want {
		t.Errorf("real World: %q (%v), grpc-go: %q", got, err, want)
	}
	c, err := dstgrpc.NewClientExact(w, "127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := c.(*grpc.ClientConn); !ok {
		t.Errorf("the real World's NewClientExact gave a %T, want the *grpc.ClientConn", c)
	}
	c.Close()
	for seed := uint64(1); seed <= 10; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		srv := sim.Machine("srv")
		var got string
		res := sim.Run(func(w *dst.World) { got, err = interceptorRun(srv.World, w, "srv:50051", true) })
		if res.Err != nil || err != nil || got != want {
			t.Fatalf("sim seed %d: %q (%v %v), grpc-go: %q", seed, got, res.Err, err, want)
		}
	}
}

// Verifies: [exact-is-grpc]
// A client without transport credentials fails in both Worlds with
// grpc's own error, and a server given two unary interceptors panics in
// both with grpc's own message.
// Fails if either World accepts what grpc refuses (dstgrpc.NewClient's
// insecure default did), or refuses it differently.
func TestExactRefusesAsGRPC(t *testing.T) {
	w := realworld.New(realworld.Options{})
	_, want := grpc.NewClient("127.0.0.1:1")
	if want == nil {
		t.Fatal("grpc-go accepted a client without credentials")
	}
	if _, err := dstgrpc.NewClientExact(w, "127.0.0.1:1"); fmt.Sprint(err) != want.Error() {
		t.Errorf("real World: %v, grpc-go: %v", err, want)
	}
	panicOf := func(f func()) (p any) {
		defer func() { p = recover() }()
		f()
		return nil
	}
	i := func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return h(ctx, req)
	}
	wantPanic := panicOf(func() { grpc.NewServer(grpc.UnaryInterceptor(i), grpc.UnaryInterceptor(i)) })
	if wantPanic == nil {
		t.Fatal("grpc-go took two unary interceptors")
	}
	if p := panicOf(func() {
		dstgrpc.NewServerExact(w, dstgrpc.UnaryInterceptorOption(i), dstgrpc.UnaryInterceptorOption(i))
	}); fmt.Sprint(p) != fmt.Sprint(wantPanic) {
		t.Errorf("real World panicked with %v, grpc-go with %v", p, wantPanic)
	}
	var err error
	var p any
	res := simworld.Run(simworld.Options{Seed: 1}, func(w *dst.World) {
		_, err = dstgrpc.NewClientExact(w, "srv:50051")
		p = panicOf(func() {
			dstgrpc.NewServerExact(w, dstgrpc.UnaryInterceptorOption(i), dstgrpc.UnaryInterceptorOption(i))
		})
	})
	if res.Err != nil {
		t.Fatal(res.Err)
	}
	if fmt.Sprint(err) != want.Error() {
		t.Errorf("simulated World: %v, grpc-go: %v", err, want)
	}
	if fmt.Sprint(p) != fmt.Sprint(wantPanic) {
		t.Errorf("simulated World panicked with %v, grpc-go with %v", p, wantPanic)
	}
}

// Verifies: [graceful-stop-waits]
// GracefulStop waits for a call in progress to finish and then refuses
// new ones, in both Worlds.
// Fails if GracefulStop returns while the call still runs, or a call
// after it succeeds.
func TestGracefulStopWaits(t *testing.T) {
	run := func(srv, cli *dst.World, addr string) error {
		ln, err := srv.Net().Listen("tcp", addr)
		if err != nil {
			return err
		}
		release := dst.MakeChan[struct{}](srv, 0)
		var finished atomic.Bool
		s := dstgrpc.NewServerExact(srv, dstgrpc.UnaryInterceptorOption(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			release.Recv()
			defer finished.Store(true)
			return h(ctx, req)
		}))
		hv1.RegisterHealthServer(s, &health{})
		srv.Go(func() { s.Serve(ln) })
		c, err := dstgrpc.NewClientExact(cli, ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return err
		}
		defer c.Close()
		hc := hv1.NewHealthClient(c)
		callDone := dst.MakeChan[error](cli, 1)
		cli.Go(func() {
			_, err := hc.Check(context.Background(), &hv1.HealthCheckRequest{})
			callDone.Send(err)
		})
		cli.Sleep(100 * time.Millisecond) // the call reaches the server
		stopped := dst.MakeChan[bool](srv, 1)
		srv.Go(func() { s.GracefulStop(); stopped.Send(finished.Load()) })
		cli.Sleep(100 * time.Millisecond)
		release.Send(struct{}{})
		if ok, _ := stopped.Recv(); !ok {
			return fmt.Errorf("GracefulStop returned before the call finished")
		}
		if err, _ := callDone.Recv(); err != nil {
			return fmt.Errorf("the call in progress failed: %v", err)
		}
		ctx, cancel := dst.WithTimeout(cli, context.Background(), time.Second)
		defer cancel()
		if _, err := hc.Check(ctx, &hv1.HealthCheckRequest{}); err == nil {
			return fmt.Errorf("a call after GracefulStop succeeded")
		}
		return nil
	}
	w := realworld.New(realworld.Options{})
	if err := run(w, w, "127.0.0.1:0"); err != nil {
		t.Fatalf("real: %v", err)
	}
	for seed := uint64(1); seed <= 10; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		srv := sim.Machine("srv")
		var err error
		res := sim.Run(func(w *dst.World) { err = run(srv.World, w, "srv:50051") })
		if res.Err != nil || err != nil {
			t.Fatalf("sim seed %d: %v %v", seed, res.Err, err)
		}
	}
}

// Many goroutines call through one NewClientExact connection at once
// (under -race in CI), with chained interceptors on both sides: every
// call succeeds and every interceptor sees every call, exactly once.
// Fails if a call fails or an interceptor misses or repeats a call under
// concurrency, in either World.
func TestStressExactConcurrentCalls(t *testing.T) {
	const goroutines, calls = 8, 25
	run := func(srv, cli *dst.World, addr string) error {
		var su, sc, cu, cc atomic.Int64
		count := func(n *atomic.Int64) grpc.UnaryServerInterceptor {
			return func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
				n.Add(1)
				return h(ctx, req)
			}
		}
		ccount := func(n *atomic.Int64) grpc.UnaryClientInterceptor {
			return func(ctx context.Context, m string, req, reply any, c *grpc.ClientConn, inv grpc.UnaryInvoker, o ...grpc.CallOption) error {
				n.Add(1)
				return inv(ctx, m, req, reply, c, o...)
			}
		}
		ln, err := srv.Net().Listen("tcp", addr)
		if err != nil {
			return err
		}
		s := dstgrpc.NewServerExact(srv, dstgrpc.UnaryInterceptorOption(count(&su)), dstgrpc.ChainUnaryInterceptorOption(count(&sc)))
		hv1.RegisterHealthServer(s, &health{})
		srv.Go(func() { s.Serve(ln) })
		defer s.Stop()
		c, err := dstgrpc.NewClientExact(cli, ln.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
			dstgrpc.WithUnaryInterceptorOption(ccount(&cu)), dstgrpc.WithChainUnaryInterceptorOption(ccount(&cc)))
		if err != nil {
			return err
		}
		defer c.Close()
		hc := hv1.NewHealthClient(c)
		var wg dst.WaitGroup
		var failed atomic.Int64
		for range goroutines {
			wg.Go(cli, func() {
				for range calls {
					if _, err := hc.Check(context.Background(), &hv1.HealthCheckRequest{}); err != nil {
						failed.Add(1)
					}
				}
			})
		}
		wg.Wait()
		const n = goroutines * calls
		if failed.Load() != 0 || su.Load() != n || sc.Load() != n || cu.Load() != n || cc.Load() != n {
			return fmt.Errorf("%d failed; interceptors saw server %d/%d client %d/%d of %d", failed.Load(), su.Load(), sc.Load(), cu.Load(), cc.Load(), n)
		}
		return nil
	}
	w := realworld.New(realworld.Options{})
	if err := run(w, w, "127.0.0.1:0"); err != nil {
		t.Fatalf("real: %v", err)
	}
	for seed := uint64(1); seed <= 3; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		srv := sim.Machine("srv")
		var err error
		res := sim.Run(func(w *dst.World) { err = run(srv.World, w, "srv:50051") })
		if res.Err != nil || err != nil {
			t.Fatalf("sim seed %d: %v %v", seed, res.Err, err)
		}
	}
}
