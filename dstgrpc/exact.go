package dstgrpc

import (
	"google.golang.org/grpc"

	"github.com/prince776/go-dst-kit/dst"
)

// NewServerExact is grpc.NewServer for w, taking grpc's own options: in
// the real World it is grpc.NewServer(opts...) itself, untouched. In a
// simulated World the options are checked by grpc.NewServer (so what
// panics there panics here), and the interceptors made with the
// options below (UnaryInterceptorOption, ChainUnaryInterceptorOption,
// ...) run in grpc's order; other options (credentials, message sizes,
// keepalive) don't apply to the simulated transport. dstmigrate rewrites
// grpc.UnaryInterceptor and the rest to these, and reports options it
// can't see through.
func NewServerExact(w *dst.World, opts ...grpc.ServerOption) *Server {
	if !w.Simulated() {
		return &Server{w: w, real: grpc.NewServer(opts...), services: map[string]*service{}}
	}
	grpc.NewServer(opts...).Stop() // grpc's own checks, its panics
	s := &Server{w: w, services: map[string]*service{}}
	var unary grpc.UnaryServerInterceptor
	var stream grpc.StreamServerInterceptor
	var chainU []grpc.UnaryServerInterceptor
	var chainS []grpc.StreamServerInterceptor
	for _, o := range opts {
		if c, ok := o.(serverOption); ok {
			if c.unary != nil {
				unary = c.unary
			}
			if c.stream != nil {
				stream = c.stream
			}
			chainU = append(chainU, c.chainUnary...)
			chainS = append(chainS, c.chainStream...)
		}
	}
	// grpc's order: the single interceptor outermost, then the chain.
	if unary != nil {
		chainU = append([]grpc.UnaryServerInterceptor{unary}, chainU...)
	}
	if stream != nil {
		chainS = append([]grpc.StreamServerInterceptor{stream}, chainS...)
	}
	for _, i := range chainU {
		s.unary = chainUnary(s.unary, i)
	}
	for _, i := range chainS {
		s.stream = chainStream(s.stream, i)
	}
	return s
}

// serverOption is a grpc.ServerOption that also carries its interceptors
// for a simulated World.
type serverOption struct {
	grpc.ServerOption
	unary       grpc.UnaryServerInterceptor
	stream      grpc.StreamServerInterceptor
	chainUnary  []grpc.UnaryServerInterceptor
	chainStream []grpc.StreamServerInterceptor
}

// UnaryInterceptorOption is grpc.UnaryInterceptor, carried into a
// simulated World by NewServerExact.
func UnaryInterceptorOption(i grpc.UnaryServerInterceptor) grpc.ServerOption {
	return serverOption{ServerOption: grpc.UnaryInterceptor(i), unary: i}
}

// StreamInterceptorOption is grpc.StreamInterceptor, carried into a
// simulated World by NewServerExact.
func StreamInterceptorOption(i grpc.StreamServerInterceptor) grpc.ServerOption {
	return serverOption{ServerOption: grpc.StreamInterceptor(i), stream: i}
}

// ChainUnaryInterceptorOption is grpc.ChainUnaryInterceptor, carried
// into a simulated World by NewServerExact.
func ChainUnaryInterceptorOption(is ...grpc.UnaryServerInterceptor) grpc.ServerOption {
	return serverOption{ServerOption: grpc.ChainUnaryInterceptor(is...), chainUnary: is}
}

// ChainStreamInterceptorOption is grpc.ChainStreamInterceptor, carried
// into a simulated World by NewServerExact.
func ChainStreamInterceptorOption(is ...grpc.StreamServerInterceptor) grpc.ServerOption {
	return serverOption{ServerOption: grpc.ChainStreamInterceptor(is...), chainStream: is}
}

// NewClientExact is grpc.NewClient for w, taking grpc's own options: in
// the real World it is grpc.NewClient(target, opts...) itself, with no
// default added (no credentials fails as it does there). In a simulated
// World the options are checked by grpc.NewClient, which doesn't
// connect (so its errors are grpc's, word for word: "no transport
// security set" among them), and the interceptors made with the options
// below run in grpc's order; other options don't apply to the simulated
// transport.
func NewClientExact(w *dst.World, target string, opts ...grpc.DialOption) (ClientConn, error) {
	if !w.Simulated() {
		cc, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, err // not a typed-nil *grpc.ClientConn
		}
		return cc, nil
	}
	check, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	check.Close()
	cc := &client{w: w, target: target}
	var unary grpc.UnaryClientInterceptor
	var stream grpc.StreamClientInterceptor
	var chainU []grpc.UnaryClientInterceptor
	var chainS []grpc.StreamClientInterceptor
	for _, o := range opts {
		if c, ok := o.(dialOption); ok {
			if c.unary != nil {
				unary = c.unary // a later one replaces it, as in grpc
			}
			if c.stream != nil {
				stream = c.stream
			}
			chainU = append(chainU, c.chainUnary...)
			chainS = append(chainS, c.chainStream...)
		}
	}
	if unary != nil {
		chainU = append([]grpc.UnaryClientInterceptor{unary}, chainU...)
	}
	if stream != nil {
		chainS = append([]grpc.StreamClientInterceptor{stream}, chainS...)
	}
	for _, i := range chainU {
		cc.unary = chainUnaryClient(cc.unary, i)
	}
	for _, i := range chainS {
		cc.stream = chainStreamClient(cc.stream, i)
	}
	return cc, nil
}

// dialOption is a grpc.DialOption that also carries its interceptors for
// a simulated World.
type dialOption struct {
	grpc.DialOption
	unary       grpc.UnaryClientInterceptor
	stream      grpc.StreamClientInterceptor
	chainUnary  []grpc.UnaryClientInterceptor
	chainStream []grpc.StreamClientInterceptor
}

// WithUnaryInterceptorOption is grpc.WithUnaryInterceptor, carried into
// a simulated World by NewClientExact.
func WithUnaryInterceptorOption(i grpc.UnaryClientInterceptor) grpc.DialOption {
	return dialOption{DialOption: grpc.WithUnaryInterceptor(i), unary: i}
}

// WithStreamInterceptorOption is grpc.WithStreamInterceptor, carried
// into a simulated World by NewClientExact.
func WithStreamInterceptorOption(i grpc.StreamClientInterceptor) grpc.DialOption {
	return dialOption{DialOption: grpc.WithStreamInterceptor(i), stream: i}
}

// WithChainUnaryInterceptorOption is grpc.WithChainUnaryInterceptor,
// carried into a simulated World by NewClientExact.
func WithChainUnaryInterceptorOption(is ...grpc.UnaryClientInterceptor) grpc.DialOption {
	return dialOption{DialOption: grpc.WithChainUnaryInterceptor(is...), chainUnary: is}
}

// WithChainStreamInterceptorOption is grpc.WithChainStreamInterceptor,
// carried into a simulated World by NewClientExact.
func WithChainStreamInterceptorOption(is ...grpc.StreamClientInterceptor) grpc.DialOption {
	return dialOption{DialOption: grpc.WithChainStreamInterceptor(is...), chainStream: is}
}
