package grottoserver

import (
	"context"
	"log"
	"net"
	"net/http"
	"strings"

	"github.com/gorilla/websocket"
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
	services map[string]serviceInfo
	logger   *log.Logger
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

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

func NewServer(opts ...Option) *GrottoServer {
	s := &GrottoServer{
		services: make(map[string]serviceInfo),
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

// ParseMethodPath splits a gRPC method path from a WebSocket request URL
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

	ws, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logf("grotto: upgrade failed remote=%s path=%s: %v", r.RemoteAddr, r.URL.Path, err)
		return
	}

	kind := "unary"
	if stream != nil {
		kind = "stream"
	}
	s.logf("grotto: rpc start remote=%s path=%s kind=%s", r.RemoteAddr, r.URL.Path, kind)

	var serveErr error
	if method != nil {
		serveErr = s.serveUnary(ws, info.impl, *method)
	} else {
		serveErr = s.serveStream(ws, info.impl, *stream)
	}
	if serveErr != nil {
		s.logf("grotto: rpc error remote=%s path=%s: %v", r.RemoteAddr, r.URL.Path, serveErr)
		return
	}
	s.logf("grotto: rpc ok remote=%s path=%s", r.RemoteAddr, r.URL.Path)
}

func (s *GrottoServer) serveUnary(ws *websocket.Conn, impl any, method grpc.MethodDesc) error {
	conn := newRPCConn(ws)
	if err := conn.consumeUnaryRequest(); err != nil {
		return conn.finishError(err)
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

func (s *GrottoServer) serveStream(ws *websocket.Conn, impl any, desc grpc.StreamDesc) error {
	conn := newRPCConn(ws)
	stream := &grpcServerStream{conn: conn}
	if err := desc.Handler(impl, stream); err != nil {
		return conn.finishError(err)
	}
	return conn.finishOK()
}

// grpcServerStream adapts an RPC WebSocket to grpc.ServerStream.
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
