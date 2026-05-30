package grottoserver

import (
	"context"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

type serviceInfo struct {
	impl    any
	methods map[string]grpc.MethodDesc
	streams map[string]grpc.StreamDesc
}

type GrottoServer struct {
	services     map[string]serviceInfo
	logger       *log.Logger
	bidiSessions *bidiSessionRegistry
}

var _ grpc.ServiceRegistrar = (*GrottoServer)(nil)

// Option configures a GrottoServer.
type Option func(*GrottoServer)

// WithLogger enables RPC logging to logger. Pass nil to disable.
func WithLogger(logger *log.Logger) Option {
	return func(s *GrottoServer) {
		s.logger = logger
	}
}

// WithLogging enables or disables RPC logging using the standard library logger.
func WithLogging(enabled bool) Option {
	return func(s *GrottoServer) {
		if enabled {
			s.logger = log.Default()
		} else {
			s.logger = nil
		}
	}
}

func NewServer(opts ...Option) *GrottoServer {
	s := &GrottoServer{
		services:     make(map[string]serviceInfo),
		bidiSessions: newBidiSessionRegistry(),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

func (s *GrottoServer) logf(format string, args ...any) {
	if s.logger != nil {
		s.logger.Printf(format, args...)
	}
}

func (s *GrottoServer) RegisterService(desc *grpc.ServiceDesc, impl any) {
	methods := map[string]grpc.MethodDesc{}
	for _, method := range desc.Methods {
		methods[method.MethodName] = method
	}
	streams := map[string]grpc.StreamDesc{}
	for _, stream := range desc.Streams {
		streams[stream.StreamName] = stream
	}
	s.services[desc.ServiceName] = serviceInfo{
		impl:    impl,
		methods: methods,
		streams: streams,
	}
}

// ParseMethodPath splits a gRPC method path from an HTTP request URL
// (e.g. "/ping.PingService/Ping") into service and method names.
func ParseMethodPath(path string) (service, method string, ok bool) {
	path = strings.Trim(path, "/")
	if path == "" {
		return "", "", false
	}
	i := strings.LastIndex(path, "/")
	if i <= 0 || i == len(path)-1 {
		return "", "", false
	}
	service = path[:i]
	method = path[i+1:]
	if service == "" || method == "" {
		return "", "", false
	}
	return service, method, true
}

func (s *GrottoServer) lookupRoute(path string) (info serviceInfo, method *grpc.MethodDesc, stream *grpc.StreamDesc, ok bool) {
	serviceName, methodName, ok := ParseMethodPath(path)
	if !ok {
		return serviceInfo{}, nil, nil, false
	}
	info, ok = s.services[serviceName]
	if !ok {
		return serviceInfo{}, nil, nil, false
	}
	if m, ok := info.methods[methodName]; ok {
		return info, &m, nil, true
	}
	if st, ok := info.streams[methodName]; ok {
		return info, nil, &st, true
	}
	return serviceInfo{}, nil, nil, false
}

func (s *GrottoServer) Serve(l net.Listener) error {
	return (&http.Server{Handler: s}).Serve(l)
}

func (s *GrottoServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	info, method, stream, ok := s.lookupRoute(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	sessionID := r.Header.Get(HeaderSessionID)
	isBidi := stream != nil && stream.ClientStreams && stream.ServerStreams

	if isBidi {
		kind := "bidi-server"
		if sessionID != "" {
			kind = "bidi-client"
		}
		s.logf("grotto: rpc start remote=%s path=%s kind=%s session=%s", r.RemoteAddr, r.URL.Path, kind, sessionID)

		ctx := r.Context()
		var serveErr error
		if sessionID == "" {
			serveErr = s.serveBidiServerLeg(ctx, r.Body, w, info.impl, *stream)
		} else {
			serveErr = s.serveBidiClientLeg(ctx, r.Body, w, sessionID)
		}
		if serveErr != nil {
			s.logf("grotto: rpc error remote=%s path=%s: %v", r.RemoteAddr, r.URL.Path, serveErr)
		} else {
			s.logf("grotto: rpc ok remote=%s path=%s", r.RemoteAddr, r.URL.Path)
		}
		return
	}

	setGrottoResponseHeaders(w)
	w.WriteHeader(http.StatusOK)

	kind := "unary"
	if stream != nil {
		kind = "stream"
	}
	s.logf("grotto: rpc start remote=%s path=%s kind=%s", r.RemoteAddr, r.URL.Path, kind)

	ctx := r.Context()
	var serveErr error
	if method != nil {
		serveErr = s.serveUnary(ctx, r.Body, w, info.impl, *method)
	} else {
		serveErr = s.serveStream(ctx, r.Body, w, info.impl, *stream)
	}
	if serveErr != nil {
		s.logf("grotto: rpc error remote=%s path=%s: %v", r.RemoteAddr, r.URL.Path, serveErr)
		return
	}
	s.logf("grotto: rpc ok remote=%s path=%s", r.RemoteAddr, r.URL.Path)
}

func (s *GrottoServer) serveUnary(ctx context.Context, body io.Reader, w http.ResponseWriter, impl any, method grpc.MethodDesc) error {
	conn := newRPCConn(ctx, body, w)
	if err := conn.consumeUnaryRequest(); err != nil {
		return conn.finishError(err)
	}
	if !conn.requestReceived {
		return conn.finishError(errors.New("client closed stream without sending a request message"))
	}

	requestBody := conn.requestBody
	dec := func(iface any) error {
		msg, ok := iface.(proto.Message)
		if !ok {
			return errNotProto
		}
		return proto.Unmarshal(requestBody, msg)
	}

	resp, err := method.Handler(impl, conn.Context(), dec, nil)
	if err != nil {
		return conn.finishError(err)
	}

	if err := conn.SendMsg(resp); err != nil {
		return conn.finishError(err)
	}
	return conn.finishOK()
}

func (s *GrottoServer) serveStream(ctx context.Context, body io.Reader, w http.ResponseWriter, impl any, desc grpc.StreamDesc) error {
	conn := newRPCConn(ctx, body, w)
	if desc.ClientStreams && !desc.ServerStreams {
		if err := conn.consumeStreamHeaders(); err != nil {
			return conn.finishError(err)
		}
	}
	if desc.ServerStreams && !desc.ClientStreams {
		if err := conn.consumeServerStreamRequest(); err != nil {
			return conn.finishError(err)
		}
		if !conn.requestReceived {
			return conn.finishError(errors.New("client closed stream without sending a request message"))
		}
	}
	stream := &grpcServerStream{conn: conn}
	if err := desc.Handler(impl, stream); err != nil {
		return conn.finishError(err)
	}
	return conn.finishOK()
}

// grpcServerStream adapts an HTTP streaming RPC to grpc.ServerStream.
type grpcServerStream struct {
	conn *rpcConn
}

func (s *grpcServerStream) SetHeader(md metadata.MD) error {
	return s.conn.SetHeader(md)
}

func (s *grpcServerStream) SendHeader(md metadata.MD) error {
	return s.conn.SendHeader(md)
}

func (s *grpcServerStream) SetTrailer(md metadata.MD) {
	s.conn.SetTrailer(md)
}

func (s *grpcServerStream) Context() context.Context {
	return s.conn.Context()
}

func (s *grpcServerStream) SendMsg(m any) error {
	return s.conn.SendMsg(m)
}

func (s *grpcServerStream) RecvMsg(m any) error {
	return s.conn.RecvMsg(m)
}
