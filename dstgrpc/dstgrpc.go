// Package dstgrpc runs gRPC services and clients in a World. Your
// generated code and service implementations stay unchanged:
//
//	s := dstgrpc.NewServer(w)              // instead of grpc.NewServer()
//	pb.RegisterFooServer(s, impl)          // unchanged
//	go s.Serve(ln)                         // ln from w.Net().Listen
//
//	conn, _ := dstgrpc.NewClient(w, "db-1:50051") // instead of grpc.NewClient
//	client := pb.NewFooClient(conn)               // unchanged
//
// In the real World these are grpc-go's server and client. In a simulated
// World, Server implements grpc.ServiceRegistrar and the client implements
// grpc.ClientConnInterface over the World's network with a compact framing
// (one connection per call), so faults, latency and deadlines follow the
// seed and the World's clock. Unary and streaming calls, metadata
// (headers and trailers), status codes and interceptors are supported;
// status details are not carried.
//
// Verified:
//   - [services-run-unchanged] The same generated code and implementation
//     work in both Worlds: unary and streaming calls, status codes,
//     metadata.
//   - [deadline-propagates] A call's deadline reaches the server's context
//     and ends the call with DeadlineExceeded, even if the server never
//     answers.
//   - [interceptors-run] Unary and stream interceptors wrap every call, on
//     both sides.
//
// Depends on: dst, google.golang.org/grpc
package dstgrpc

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/prince776/go-dst-kit/dst"
)

// ---- options ----

// ServerOption configures a Server.
type ServerOption struct {
	unary  grpc.UnaryServerInterceptor
	stream grpc.StreamServerInterceptor
	real   []grpc.ServerOption
}

// UnaryInterceptor installs a unary server interceptor (both Worlds).
func UnaryInterceptor(i grpc.UnaryServerInterceptor) ServerOption { return ServerOption{unary: i} }

// StreamInterceptor installs a stream server interceptor (both Worlds).
func StreamInterceptor(i grpc.StreamServerInterceptor) ServerOption { return ServerOption{stream: i} }

// RealServerOptions passes grpc-go options through in the real World; the
// simulated World ignores them.
func RealServerOptions(opts ...grpc.ServerOption) ServerOption { return ServerOption{real: opts} }

// DialOption configures a client.
type DialOption struct {
	unary  grpc.UnaryClientInterceptor
	stream grpc.StreamClientInterceptor
	real   []grpc.DialOption
}

// WithUnaryInterceptor installs a unary client interceptor (both Worlds).
func WithUnaryInterceptor(i grpc.UnaryClientInterceptor) DialOption { return DialOption{unary: i} }

// WithStreamInterceptor installs a stream client interceptor (both Worlds).
func WithStreamInterceptor(i grpc.StreamClientInterceptor) DialOption { return DialOption{stream: i} }

// RealDialOptions passes grpc-go options through in the real World (the
// default there is insecure transport credentials).
func RealDialOptions(opts ...grpc.DialOption) DialOption { return DialOption{real: opts} }

// ---- server ----

// Server is a gRPC server for a World.
type Server struct {
	w        *dst.World
	real     *grpc.Server
	unary    grpc.UnaryServerInterceptor
	stream   grpc.StreamServerInterceptor
	services map[string]*service
	lis      []net.Listener
}

type service struct {
	impl    any
	methods map[string]grpc.MethodDesc
	streams map[string]grpc.StreamDesc
}

// NewServer returns a server for w.
func NewServer(w *dst.World, opts ...ServerOption) *Server {
	s := &Server{w: w, services: map[string]*service{}}
	var real []grpc.ServerOption
	for _, o := range opts {
		if o.unary != nil {
			s.unary = chainUnary(s.unary, o.unary)
			real = append(real, grpc.ChainUnaryInterceptor(o.unary))
		}
		if o.stream != nil {
			s.stream = chainStream(s.stream, o.stream)
			real = append(real, grpc.ChainStreamInterceptor(o.stream))
		}
		real = append(real, o.real...)
	}
	if !w.Simulated() {
		s.real = grpc.NewServer(real...)
	}
	return s
}

// RegisterService implements grpc.ServiceRegistrar.
func (s *Server) RegisterService(desc *grpc.ServiceDesc, impl any) {
	if s.real != nil {
		s.real.RegisterService(desc, impl)
		return
	}
	svc := &service{impl: impl, methods: map[string]grpc.MethodDesc{}, streams: map[string]grpc.StreamDesc{}}
	for _, m := range desc.Methods {
		svc.methods[m.MethodName] = m
	}
	for _, st := range desc.Streams {
		svc.streams[st.StreamName] = st
	}
	s.services[desc.ServiceName] = svc
}

// Serve accepts connections on ln until it fails or the server stops.
func (s *Server) Serve(ln net.Listener) error {
	if s.real != nil {
		return s.real.Serve(ln)
	}
	s.lis = append(s.lis, ln)
	for {
		c, err := ln.Accept()
		if err != nil {
			return err
		}
		s.w.GoNamed("grpc "+c.RemoteAddr().String(), func() { s.serveCall(c) })
	}
}

// Stop closes the listeners (and in the real World, every connection).
func (s *Server) Stop() {
	if s.real != nil {
		s.real.Stop()
		return
	}
	for _, ln := range s.lis {
		ln.Close()
	}
}

// Frames: a type byte, a 4-byte length, the payload.
const (
	fCallHeader = 'H' // client: method, timeout, metadata
	fMessage    = 'M' // either side: one message
	fCloseSend  = 'E' // client: no more messages
	fHeader     = 'h' // server: response header metadata
	fTrailer    = 't' // server: status and trailer metadata
)

type framer struct {
	c  net.Conn
	br *bufio.Reader
}

func (f *framer) write(t byte, payload []byte) error {
	buf := make([]byte, 5+len(payload))
	buf[0] = t
	binary.BigEndian.PutUint32(buf[1:], uint32(len(payload)))
	copy(buf[5:], payload)
	_, err := f.c.Write(buf)
	return err
}

func (f *framer) read() (byte, []byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(f.br, hdr[:]); err != nil {
		return 0, nil, err
	}
	p := make([]byte, binary.BigEndian.Uint32(hdr[1:]))
	if _, err := io.ReadFull(f.br, p); err != nil {
		return 0, nil, err
	}
	return hdr[0], p, nil
}

// Metadata and strings are encoded as length-prefixed strings.
func putString(b []byte, s string) []byte {
	b = binary.BigEndian.AppendUint32(b, uint32(len(s)))
	return append(b, s...)
}

func getString(b []byte) (string, []byte, error) {
	if len(b) < 4 {
		return "", nil, errors.New("dstgrpc: short frame")
	}
	n := binary.BigEndian.Uint32(b)
	if uint32(len(b)-4) < n {
		return "", nil, errors.New("dstgrpc: short frame")
	}
	return string(b[4 : 4+n]), b[4+n:], nil
}

func putMD(b []byte, md metadata.MD) []byte {
	keys := make([]string, 0, len(md))
	for k := range md {
		keys = append(keys, k)
	}
	sortStrings(keys)
	n := 0
	for _, k := range keys {
		n += len(md[k])
	}
	b = binary.BigEndian.AppendUint32(b, uint32(n))
	for _, k := range keys {
		for _, v := range md[k] {
			b = putString(putString(b, k), v)
		}
	}
	return b
}

func getMD(b []byte) (metadata.MD, []byte, error) {
	if len(b) < 4 {
		return nil, nil, errors.New("dstgrpc: short frame")
	}
	n := binary.BigEndian.Uint32(b)
	b = b[4:]
	md := metadata.MD{}
	for range n {
		var k, v string
		var err error
		if k, b, err = getString(b); err != nil {
			return nil, nil, err
		}
		if v, b, err = getString(b); err != nil {
			return nil, nil, err
		}
		md.Append(k, v)
	}
	return md, b, nil
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}

func marshal(m any) ([]byte, error) {
	pm, ok := m.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("dstgrpc: %T is not a proto message", m)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(pm)
}

func unmarshal(b []byte, m any) error {
	pm, ok := m.(proto.Message)
	if !ok {
		return fmt.Errorf("dstgrpc: %T is not a proto message", m)
	}
	return proto.Unmarshal(b, pm)
}

// serverCall is one call on the server: it implements grpc.ServerStream
// and grpc.ServerTransportStream.
type serverCall struct {
	f          *framer
	ctx        context.Context
	method     string
	header     metadata.MD
	trailer    metadata.MD
	headerSent bool
	closed     bool // the client sent fCloseSend
}

func (s *Server) serveCall(c net.Conn) {
	defer c.Close()
	f := &framer{c: c, br: bufio.NewReader(c)}
	t, p, err := f.read()
	if err != nil || t != fCallHeader {
		return
	}
	method, rest, err := getString(p)
	if err != nil || len(rest) < 8 {
		return
	}
	timeout := time.Duration(binary.BigEndian.Uint64(rest))
	md, _, err := getMD(rest[8:])
	if err != nil {
		return
	}
	ctx, cancel := dst.WithCancel(s.w, context.Background())
	defer cancel()
	if timeout > 0 {
		ctx, cancel = dst.WithTimeout(s.w, ctx, timeout)
		defer cancel()
	}
	call := &serverCall{f: f, method: method, header: metadata.MD{}, trailer: metadata.MD{}}
	var watcherDone dst.Chan[struct{}] // set when a unary call's watcher reads the rest
	ctx = metadata.NewIncomingContext(ctx, md)
	ctx = grpc.NewContextWithServerTransportStream(ctx, transportStream{call})
	call.ctx = ctx

	svcName, mName, ok := splitMethod(method)
	svc := s.services[svcName]
	var reply any
	var handlerErr error
	switch {
	case !ok || svc == nil:
		handlerErr = status.Errorf(codes.Unimplemented, "unknown service %s", svcName)
	case hasMethod(svc, mName):
		md := svc.methods[mName]
		// The client may cancel (close the connection) while the handler
		// runs: watch for that, after reading the request.
		dec := func(m any) error {
			t, p, err := f.read()
			if err != nil {
				return status.Error(codes.Canceled, "client went away")
			}
			if t != fMessage {
				return status.Error(codes.Internal, "expected a request message")
			}
			if err := unmarshal(p, m); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
			watcherDone = dst.MakeChan[struct{}](s.w, 0)
			s.w.Go(func() {
				defer watcherDone.Close()
				for {
					if _, _, err := f.read(); err != nil {
						cancel() // the client hung up
						return
					}
				}
			})
			return nil
		}
		reply, handlerErr = md.Handler(svc.impl, ctx, dec, s.unary)
	case hasStream(svc, mName):
		sd := svc.streams[mName]
		if s.stream != nil {
			info := &grpc.StreamServerInfo{FullMethod: method, IsClientStream: sd.ClientStreams, IsServerStream: sd.ServerStreams}
			handlerErr = s.stream(svc.impl, call, info, sd.Handler)
		} else {
			handlerErr = sd.Handler(svc.impl, call)
		}
	default:
		handlerErr = status.Errorf(codes.Unimplemented, "unknown method %s", mName)
	}
	call.sendHeader()
	if handlerErr == nil && reply != nil {
		b, err := marshal(reply)
		if err != nil {
			handlerErr = status.Error(codes.Internal, err.Error())
		} else {
			f.write(fMessage, b)
		}
	}
	st := status.Convert(handlerErr)
	if handlerErr != nil && ctx.Err() == context.DeadlineExceeded && st.Code() == codes.Unknown {
		st = status.New(codes.DeadlineExceeded, ctx.Err().Error())
	}
	tb := binary.BigEndian.AppendUint32(nil, uint32(st.Code()))
	tb = putString(tb, st.Message())
	tb = putMD(tb, call.trailer)
	f.write(fTrailer, tb)
	// Linger: let the client read the response and close first. Closing
	// with its frames unread would reset the connection and could destroy
	// the response in flight.
	c.SetReadDeadline(s.w.Now().Add(lingerTimeout))
	if watcherDone != (dst.Chan[struct{}]{}) {
		watcherDone.Recv()
		return
	}
	for {
		if _, _, err := f.read(); err != nil {
			return
		}
	}
}

// lingerTimeout bounds how long a finished call waits for its client.
const lingerTimeout = 30 * time.Second

func hasMethod(s *service, name string) bool { _, ok := s.methods[name]; return ok }
func hasStream(s *service, name string) bool { _, ok := s.streams[name]; return ok }

func splitMethod(m string) (svc, method string, ok bool) {
	m = strings.TrimPrefix(m, "/")
	i := strings.LastIndex(m, "/")
	if i < 0 {
		return "", "", false
	}
	return m[:i], m[i+1:], true
}

// transportStream is what grpc.SetHeader/SendHeader/SetTrailer find in
// the context (its SetTrailer returns an error, unlike ServerStream's).
type transportStream struct{ c *serverCall }

func (t transportStream) Method() string                  { return t.c.method }
func (t transportStream) SetHeader(md metadata.MD) error  { return t.c.SetHeader(md) }
func (t transportStream) SendHeader(md metadata.MD) error { return t.c.SendHeader(md) }
func (t transportStream) SetTrailer(md metadata.MD) error { t.c.SetTrailer(md); return nil }

func (c *serverCall) SetHeader(md metadata.MD) error {
	if c.headerSent {
		return errors.New("dstgrpc: header already sent")
	}
	c.header = metadata.Join(c.header, md)
	return nil
}

func (c *serverCall) SendHeader(md metadata.MD) error {
	if err := c.SetHeader(md); err != nil {
		return err
	}
	return c.sendHeader()
}

func (c *serverCall) sendHeader() error {
	if c.headerSent {
		return nil
	}
	c.headerSent = true
	return c.f.write(fHeader, putMD(nil, c.header))
}

func (c *serverCall) SetTrailer(md metadata.MD) {
	c.trailer = metadata.Join(c.trailer, md)
}

func (c *serverCall) Context() context.Context { return c.ctx }

func (c *serverCall) SendMsg(m any) error {
	if err := c.sendHeader(); err != nil {
		return err
	}
	b, err := marshal(m)
	if err != nil {
		return err
	}
	return c.f.write(fMessage, b)
}

func (c *serverCall) RecvMsg(m any) error {
	if c.closed {
		return io.EOF
	}
	t, p, err := c.f.read()
	if err != nil {
		return status.Error(codes.Canceled, "client went away")
	}
	switch t {
	case fCloseSend:
		c.closed = true
		return io.EOF
	case fMessage:
		return unmarshal(p, m)
	}
	return status.Error(codes.Internal, "unexpected frame")
}

var _ grpc.ServerStream = (*serverCall)(nil)

// ---- client ----

// ClientConn is a client connection: what generated New...Client
// functions take, plus Close.
type ClientConn interface {
	grpc.ClientConnInterface
	Close() error
}

// NewClient returns a client for target ("machine:port" in a simulated
// World).
func NewClient(w *dst.World, target string, opts ...DialOption) (ClientConn, error) {
	cc := &client{w: w, target: target}
	real := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	for _, o := range opts {
		if o.unary != nil {
			cc.unary = chainUnaryClient(cc.unary, o.unary)
			real = append(real, grpc.WithChainUnaryInterceptor(o.unary))
		}
		if o.stream != nil {
			cc.stream = chainStreamClient(cc.stream, o.stream)
			real = append(real, grpc.WithChainStreamInterceptor(o.stream))
		}
		real = append(real, o.real...)
	}
	if !w.Simulated() {
		return grpc.NewClient(target, real...)
	}
	return cc, nil
}

type client struct {
	w      *dst.World
	target string
	unary  grpc.UnaryClientInterceptor
	stream grpc.StreamClientInterceptor
}

func (cc *client) Close() error { return nil }

func (cc *client) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	invoke := func(ctx context.Context, method string, args, reply any, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
		cs, err := cc.open(ctx, method)
		if err != nil {
			return err
		}
		defer cs.finish()
		if err := cs.SendMsg(args); err != nil {
			return cs.toStatus(err)
		}
		if err := cs.CloseSend(); err != nil {
			return cs.toStatus(err)
		}
		err = cs.RecvMsg(reply)
		if err == io.EOF {
			err = status.Error(codes.Internal, "no response message")
		}
		if err == nil {
			// Read through to the trailer: it carries the final status
			// and trailer metadata.
			switch _, end := cs.next(); {
			case end == nil:
				err = status.Error(codes.Internal, "more than one response message")
			case end != io.EOF:
				err = end
			}
		}
		cs.applyCallOptions(opts)
		return err
	}
	if cc.unary != nil {
		return cc.unary(ctx, method, args, reply, nil, invoke, opts...)
	}
	return invoke(ctx, method, args, reply, nil, opts...)
}

func (cc *client) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, _ *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		cs, err := cc.open(ctx, method)
		if err != nil {
			return nil, err
		}
		cs.opts = opts
		return cs, nil
	}
	if cc.stream != nil {
		return cc.stream(ctx, desc, nil, method, streamer, opts...)
	}
	return streamer(ctx, desc, nil, method, opts...)
}

var longAgo = time.Unix(1, 0)

func (cc *client) open(ctx context.Context, method string) (*clientCall, error) {
	c, err := cc.w.Net().DialContext(ctx, "tcp", cc.target)
	if err != nil {
		return nil, ctxStatus(ctx, status.Error(codes.Unavailable, err.Error()))
	}
	cs := &clientCall{ctx: ctx, f: &framer{c: c, br: bufio.NewReader(c)}}
	var timeout time.Duration
	if dl, ok := ctx.Deadline(); ok {
		c.SetDeadline(dl)
		timeout = max(cc.w.Until(dl), time.Nanosecond)
	}
	cs.stop = dst.AfterDone(ctx, func() { c.SetDeadline(longAgo) })
	md, _ := metadata.FromOutgoingContext(ctx)
	p := putString(nil, method)
	p = binary.BigEndian.AppendUint64(p, uint64(timeout))
	p = putMD(p, md)
	if err := cs.f.write(fCallHeader, p); err != nil {
		cs.finish()
		return nil, cs.toStatus(err)
	}
	return cs, nil
}

func ctxStatus(ctx context.Context, err error) error {
	switch ctx.Err() {
	case context.DeadlineExceeded:
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	case context.Canceled:
		return status.Error(codes.Canceled, "context canceled")
	}
	return err
}

// clientCall is one call on the client: it implements grpc.ClientStream.
type clientCall struct {
	ctx       context.Context
	f         *framer
	stop      func() bool
	opts      []grpc.CallOption
	header    metadata.MD
	gotHeader bool
	trailer   metadata.MD
	pending   [][]byte // messages read while waiting for the header
	done      bool
	err       error
	finished  bool
}

func (cs *clientCall) finish() {
	if !cs.finished {
		cs.finished = true
		cs.stop()
		cs.f.c.Close()
	}
}

func (cs *clientCall) toStatus(err error) error {
	if _, ok := status.FromError(err); ok && err != nil {
		return err
	}
	return ctxStatus(cs.ctx, status.Error(codes.Unavailable, err.Error()))
}

// next reads frames until a message or the end of the call.
func (cs *clientCall) next() ([]byte, error) {
	if len(cs.pending) > 0 {
		m := cs.pending[0]
		cs.pending = cs.pending[1:]
		return m, nil
	}
	for !cs.done {
		t, p, err := cs.f.read()
		if err != nil {
			cs.done, cs.err = true, cs.toStatus(err)
			break
		}
		switch t {
		case fHeader:
			cs.header, _, _ = getMD(p)
			cs.gotHeader = true
		case fMessage:
			return p, nil
		case fTrailer:
			cs.done = true
			code := codes.Code(binary.BigEndian.Uint32(p))
			msg, rest, _ := getString(p[4:])
			cs.trailer, _, _ = getMD(rest)
			if code != codes.OK {
				cs.err = status.Error(code, msg)
			} else {
				cs.err = io.EOF
			}
		}
	}
	return nil, cs.err
}

func (cs *clientCall) Header() (metadata.MD, error) {
	for !cs.gotHeader && !cs.done {
		t, p, err := cs.f.read()
		if err != nil {
			cs.done, cs.err = true, cs.toStatus(err)
			break
		}
		switch t {
		case fHeader:
			cs.header, _, _ = getMD(p)
			cs.gotHeader = true
		case fMessage:
			cs.pending = append(cs.pending, p)
		case fTrailer:
			cs.done = true
			code := codes.Code(binary.BigEndian.Uint32(p))
			msg, rest, _ := getString(p[4:])
			cs.trailer, _, _ = getMD(rest)
			if code != codes.OK {
				cs.err = status.Error(code, msg)
			} else {
				cs.err = io.EOF
			}
		}
	}
	if !cs.gotHeader && cs.err != nil && cs.err != io.EOF {
		return nil, cs.err
	}
	return cs.header, nil
}

func (cs *clientCall) Trailer() metadata.MD { return cs.trailer }

func (cs *clientCall) CloseSend() error { return cs.f.write(fCloseSend, nil) }

func (cs *clientCall) Context() context.Context { return cs.ctx }

func (cs *clientCall) SendMsg(m any) error {
	b, err := marshal(m)
	if err != nil {
		return err
	}
	if err := cs.f.write(fMessage, b); err != nil {
		return io.EOF // as grpc-go: the real error comes from RecvMsg
	}
	return nil
}

func (cs *clientCall) RecvMsg(m any) error {
	p, err := cs.next()
	if err != nil {
		if cs.done {
			cs.applyCallOptions(cs.opts)
			cs.finish()
		}
		return err
	}
	return unmarshal(p, m)
}

func (cs *clientCall) applyCallOptions(opts []grpc.CallOption) {
	for _, o := range opts {
		switch o := o.(type) {
		case grpc.HeaderCallOption:
			*o.HeaderAddr = cs.header
		case grpc.TrailerCallOption:
			*o.TrailerAddr = cs.trailer
		}
	}
}

var _ grpc.ClientStream = (*clientCall)(nil)

// ---- interceptor chaining ----

func chainUnary(a, b grpc.UnaryServerInterceptor) grpc.UnaryServerInterceptor {
	if a == nil {
		return b
	}
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		return a(ctx, req, info, func(ctx context.Context, req any) (any, error) { return b(ctx, req, info, h) })
	}
}

func chainStream(a, b grpc.StreamServerInterceptor) grpc.StreamServerInterceptor {
	if a == nil {
		return b
	}
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
		return a(srv, ss, info, func(srv any, ss grpc.ServerStream) error { return b(srv, ss, info, h) })
	}
}

func chainUnaryClient(a, b grpc.UnaryClientInterceptor) grpc.UnaryClientInterceptor {
	if a == nil {
		return b
	}
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, inv grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return a(ctx, method, req, reply, cc, func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			return b(ctx, method, req, reply, cc, inv, opts...)
		}, opts...)
	}
}

func chainStreamClient(a, b grpc.StreamClientInterceptor) grpc.StreamClientInterceptor {
	if a == nil {
		return b
	}
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, st grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return a(ctx, desc, cc, method, func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			return b(ctx, desc, cc, method, st, opts...)
		}, opts...)
	}
}
