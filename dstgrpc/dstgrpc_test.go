package dstgrpc_test

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	hv1 "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/prince776/go-dst-kit/dst"
	"github.com/prince776/go-dst-kit/dstgrpc"
	"github.com/prince776/go-dst-kit/internal/simworld"
	"github.com/prince776/go-dst-kit/realworld"
)

// health is an ordinary gRPC service implementation, unaware of worlds
// except for waiting on its context.
type health struct {
	hv1.UnimplementedHealthServer
	sawDeadline atomic.Bool
}

func (h *health) Check(ctx context.Context, req *hv1.HealthCheckRequest) (*hv1.HealthCheckResponse, error) {
	switch req.Service {
	case "missing":
		return nil, status.Error(codes.NotFound, "no such service")
	case "slow":
		// The client's deadline reached us. (Whether the context then ends
		// with DeadlineExceeded or Canceled is a race in real gRPC: the
		// client's reset may arrive first.)
		if _, ok := ctx.Deadline(); ok {
			h.sawDeadline.Store(true)
		}
		dst.Done(ctx).Recv()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	md, _ := metadata.FromIncomingContext(ctx)
	grpc.SetHeader(ctx, metadata.Pairs("x-echo", fmt.Sprint(md.Get("x-client"))))
	grpc.SetTrailer(ctx, metadata.Pairs("x-trail", "t"))
	return &hv1.HealthCheckResponse{Status: hv1.HealthCheckResponse_SERVING}, nil
}

func (h *health) Watch(req *hv1.HealthCheckRequest, s grpc.ServerStreamingServer[hv1.HealthCheckResponse]) error {
	for _, st := range []hv1.HealthCheckResponse_ServingStatus{
		hv1.HealthCheckResponse_SERVING, hv1.HealthCheckResponse_NOT_SERVING, hv1.HealthCheckResponse_SERVING,
	} {
		if err := s.Send(&hv1.HealthCheckResponse{Status: st}); err != nil {
			return err
		}
	}
	return nil
}

type counts struct{ srvUnary, srvStream, cliUnary, cliStream atomic.Int32 }

// exercise runs a server on srv and calls it from cli; target is the
// address the client dials.
func exercise(srv, cli *dst.World, listenAddr string, ctr *counts, h *health) error {
	ln, err := srv.Net().Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	s := dstgrpc.NewServer(srv,
		dstgrpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, hd grpc.UnaryHandler) (any, error) {
			ctr.srvUnary.Add(1)
			return hd(ctx, req)
		}),
		dstgrpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, hd grpc.StreamHandler) error {
			ctr.srvStream.Add(1)
			return hd(srv, ss)
		}))
	hv1.RegisterHealthServer(s, h)
	srv.Go(func() { s.Serve(ln) })
	defer s.Stop()
	conn, err := dstgrpc.NewClient(cli, ln.Addr().String(),
		dstgrpc.WithUnaryInterceptor(func(ctx context.Context, m string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctr.cliUnary.Add(1)
			return inv(ctx, m, req, reply, cc, opts...)
		}),
		dstgrpc.WithStreamInterceptor(func(ctx context.Context, d *grpc.StreamDesc, cc *grpc.ClientConn, m string, st grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctr.cliStream.Add(1)
			return st(ctx, d, cc, m, opts...)
		}))
	if err != nil {
		return err
	}
	defer conn.Close()
	c := hv1.NewHealthClient(conn)

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-client", "c1")
	var hdr, trl metadata.MD
	resp, err := c.Check(ctx, &hv1.HealthCheckRequest{}, grpc.Header(&hdr), grpc.Trailer(&trl))
	if err != nil || resp.Status != hv1.HealthCheckResponse_SERVING {
		return fmt.Errorf("Check: %v %v", resp, err)
	}
	if got := hdr.Get("x-echo"); len(got) != 1 || got[0] != "[c1]" || len(trl.Get("x-trail")) != 1 {
		return fmt.Errorf("metadata: header %v trailer %v", hdr, trl)
	}
	if _, err := c.Check(context.Background(), &hv1.HealthCheckRequest{Service: "missing"}); status.Code(err) != codes.NotFound {
		return fmt.Errorf("missing: %v", err)
	}
	stream, err := c.Watch(context.Background(), &hv1.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("Watch: %v", err)
	}
	var got []hv1.HealthCheckResponse_ServingStatus
	for {
		r, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("Watch.Recv: %v", err)
		}
		got = append(got, r.Status)
	}
	if fmt.Sprint(got) != "[SERVING NOT_SERVING SERVING]" {
		return fmt.Errorf("Watch got %v", got)
	}
	dctx, cancel := dst.WithTimeout(cli, context.Background(), 100*time.Millisecond)
	defer cancel()
	start := cli.Now()
	if _, err := c.Check(dctx, &hv1.HealthCheckRequest{Service: "slow"}); status.Code(err) != codes.DeadlineExceeded {
		return fmt.Errorf("slow: %v", err)
	}
	if took := cli.Since(start); took > 2*time.Second {
		return fmt.Errorf("slow call took %v", took)
	}
	return nil
}

func checkCounts(ctr *counts) error {
	if ctr.srvUnary.Load() < 3 || ctr.cliUnary.Load() < 3 || ctr.srvStream.Load() != 1 || ctr.cliStream.Load() != 1 {
		return fmt.Errorf("interceptor counts: server %d/%d client %d/%d", ctr.srvUnary.Load(), ctr.srvStream.Load(), ctr.cliUnary.Load(), ctr.cliStream.Load())
	}
	return nil
}

// Verifies: [services-run-unchanged], [interceptors-run], [deadline-propagates]
// Fails if: generated code and the same implementation behave differently
// in simulation than on real grpc-go.
func TestServicesRunUnchanged(t *testing.T) {
	w := realworld.New(realworld.Options{})
	var ctr counts
	h := &health{}
	if err := exercise(w, w, "127.0.0.1:0", &ctr, h); err != nil {
		t.Fatalf("real: %v", err)
	}
	if err := checkCounts(&ctr); err != nil {
		t.Fatalf("real: %v", err)
	}
	time.Sleep(50 * time.Millisecond) // let the real server notice
	if !h.sawDeadline.Load() {
		t.Fatal("real: the server's context had no deadline")
	}
	for seed := uint64(1); seed <= 20; seed++ {
		sim := simworld.New(simworld.Options{Seed: seed})
		srv := sim.Machine("srv")
		var ctr counts
		h := &health{}
		var err error
		res := sim.Run(func(w *dst.World) {
			err = exercise(srv.World, w, "srv:50051", &ctr, h)
			w.Sleep(time.Second)
		})
		if res.Err != nil || err != nil {
			t.Fatalf("sim seed %d: %v %v", seed, res.Err, err)
		}
		if err := checkCounts(&ctr); err != nil {
			t.Fatalf("sim seed %d: %v", seed, err)
		}
		if !h.sawDeadline.Load() {
			t.Fatalf("sim seed %d: the server's context had no deadline", seed)
		}
	}
}

// Negative: a partitioned server makes calls fail with the deadline, and
// without a deadline the call waits (the run's limit ends it).
func TestPartitionedCall(t *testing.T) {
	sim := simworld.New(simworld.Options{Seed: 1})
	srv := sim.Machine("srv")
	var err error
	res := sim.Run(func(w *dst.World) {
		ln, _ := srv.World.Net().Listen("tcp", "srv:50051")
		s := dstgrpc.NewServer(srv.World)
		hv1.RegisterHealthServer(s, &health{})
		srv.World.Go(func() { s.Serve(ln) })
		sim.Net.Partition([]string{"main"}, []string{"srv"})
		conn, _ := dstgrpc.NewClient(w, "srv:50051")
		ctx, cancel := dst.WithTimeout(w, context.Background(), time.Second)
		defer cancel()
		_, err = hv1.NewHealthClient(conn).Check(ctx, &hv1.HealthCheckRequest{})
	})
	if res.Err != nil || status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("run %v, call %v", res.Err, err)
	}
}
