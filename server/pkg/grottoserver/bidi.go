package grottoserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

const (
	HeaderSessionID = "Grotto-Session-Id"
	HeaderBidiLeg   = "Grotto-Bidi-Leg"

	bidiLegServer = "server"
	bidiLegClient = "client"

	bidiClientLegTimeout = 10 * time.Second
)

var (
	errBidiSessionNotFound = errors.New("grotto: bidi session not found")
	errBidiSessionExpired  = errors.New("grotto: client leg not opened within 10s")
)

type bidiSession struct {
	id string

	mu         sync.Mutex
	serverConn *rpcConn
	clientConn *rpcConn
	clientLeg  chan struct{}
	done       chan struct{}
	terminated bool
	timer      *time.Timer
}

type bidiSessionRegistry struct {
	mu       sync.Mutex
	sessions map[string]*bidiSession
}

func newBidiSessionRegistry() *bidiSessionRegistry {
	return &bidiSessionRegistry{sessions: make(map[string]*bidiSession)}
}

func (r *bidiSessionRegistry) create(serverConn *rpcConn) *bidiSession {
	id := uuid.NewString()
	s := &bidiSession{
		id:         id,
		serverConn: serverConn,
		clientLeg:  make(chan struct{}),
		done:       make(chan struct{}),
	}
	s.timer = time.AfterFunc(bidiClientLegTimeout, func() {
		s.terminate(errBidiSessionExpired)
	})

	r.mu.Lock()
	r.sessions[id] = s
	r.mu.Unlock()

	serverConn.setOnTerminate(func() { s.terminate(errors.New("grotto: server leg closed")) })
	return s
}

func (r *bidiSessionRegistry) get(id string) (*bidiSession, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.sessions[id]
	return s, ok
}

func (r *bidiSessionRegistry) remove(id string) {
	r.mu.Lock()
	delete(r.sessions, id)
	r.mu.Unlock()
}

func (s *bidiSession) attachClient(conn *rpcConn) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminated {
		return false
	}
	if s.clientConn != nil {
		return false
	}
	s.clientConn = conn
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	conn.setOnTerminate(func() { s.terminate(errors.New("grotto: client leg closed")) })
	close(s.clientLeg)
	return true
}

func (s *bidiSession) terminate(reason error) {
	s.mu.Lock()
	if s.terminated {
		s.mu.Unlock()
		return
	}
	s.terminated = true
	serverConn := s.serverConn
	clientConn := s.clientConn
	if s.timer != nil {
		s.timer.Stop()
		s.timer = nil
	}
	s.mu.Unlock()

	if serverConn != nil {
		_ = serverConn.finishError(reason)
	}
	if clientConn != nil {
		_ = clientConn.finishError(reason)
	}

	select {
	case <-s.clientLeg:
	default:
		close(s.clientLeg)
	}
	s.signalDone()
}

func (s *bidiSession) signalDone() {
	select {
	case <-s.done:
	default:
		close(s.done)
	}
}

func (s *GrottoServer) serveBidiServerLeg(
	ctx context.Context,
	body io.Reader,
	w http.ResponseWriter,
	impl any,
	desc grpc.StreamDesc,
) error {
	serverConn := newRPCConn(ctx, body, w)
	if err := serverConn.consumeStreamHeaders(); err != nil {
		return serverConn.finishError(err)
	}

	session := s.bidiSessions.create(serverConn)

	w.Header().Set(HeaderSessionID, session.id)
	w.Header().Set(HeaderBidiLeg, bidiLegServer)
	setGrottoResponseHeaders(w)
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	s.logf("grotto: bidi server leg session=%s waiting for client leg", session.id)

	select {
	case <-session.clientLeg:
	case <-ctx.Done():
		session.terminate(ctx.Err())
		<-session.done
		s.bidiSessions.remove(session.id)
		return ctx.Err()
	}

	session.mu.Lock()
	clientConn := session.clientConn
	session.mu.Unlock()
	if clientConn == nil {
		<-session.done
		s.bidiSessions.remove(session.id)
		return errBidiSessionExpired
	}

	stream := &bidiBridgeStream{
		ctx:    mergeContexts(serverConn.Context(), session.clientConn.Context()),
		recv:   session.clientConn,
		send:   serverConn,
	}
	var handlerErr error
	if err := desc.Handler(impl, stream); err != nil {
		handlerErr = err
	}

	if handlerErr != nil {
		_ = serverConn.finishError(handlerErr)
		_ = session.clientConn.finishError(handlerErr)
	} else {
		_ = serverConn.finishOK()
		_ = session.clientConn.finishOK()
	}

	session.signalDone()
	s.bidiSessions.remove(session.id)
	return handlerErr
}

func (s *GrottoServer) serveBidiClientLeg(
	ctx context.Context,
	body io.Reader,
	w http.ResponseWriter,
	sessionID string,
) error {
	session, ok := s.bidiSessions.get(sessionID)
	if !ok {
		http.Error(w, errBidiSessionNotFound.Error(), http.StatusNotFound)
		return errBidiSessionNotFound
	}

	clientConn := newRPCConn(ctx, body, w)
	if !session.attachClient(clientConn) {
		http.Error(w, "grotto: bidi session already has a client leg", http.StatusConflict)
		return errors.New("grotto: bidi session already has a client leg")
	}

	if err := clientConn.consumeStreamHeaders(); err != nil {
		session.terminate(err)
		<-session.done
		return err
	}

	w.Header().Set(HeaderSessionID, sessionID)
	w.Header().Set(HeaderBidiLeg, bidiLegClient)
	setGrottoResponseHeaders(w)
	w.WriteHeader(http.StatusOK)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}

	s.logf("grotto: bidi client leg attached session=%s", sessionID)

	<-session.done
	return nil
}

type bidiBridgeStream struct {
	ctx  context.Context
	recv *rpcConn
	send *rpcConn
}

func (s *bidiBridgeStream) SetHeader(md metadata.MD) error {
	return s.send.SetHeader(md)
}

func (s *bidiBridgeStream) SendHeader(md metadata.MD) error {
	return s.send.SendHeader(md)
}

func (s *bidiBridgeStream) SetTrailer(md metadata.MD) {
	s.send.SetTrailer(md)
}

func (s *bidiBridgeStream) Context() context.Context {
	return s.ctx
}

func (s *bidiBridgeStream) SendMsg(m any) error {
	return s.send.SendMsg(m)
}

func (s *bidiBridgeStream) RecvMsg(m any) error {
	return s.recv.RecvMsg(m)
}

func mergeContexts(a, b context.Context) context.Context {
	ctx, cancel := context.WithCancel(a)
	go func() {
		select {
		case <-b.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}
