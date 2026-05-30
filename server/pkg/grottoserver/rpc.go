package grottoserver

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type inboundFrame struct {
	typ     uint8
	payload []byte
	err     error
}

type rpcConn struct {
	writer *frameWriter
	mu     sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc

	inbound  chan inboundFrame
	readDone chan struct{}

	clientMetadata  metadata.MD
	requestBody     []byte
	requestReceived bool
	recvClosed      bool
	sentHeader      bool
	finished        bool

	onTerminate     func()
	onTerminateOnce sync.Once
}

func newRPCConn(ctx context.Context, body io.Reader, w http.ResponseWriter) *rpcConn {
	ctx, cancel := context.WithCancel(ctx)
	c := &rpcConn{
		writer:   newFrameWriter(w),
		ctx:      ctx,
		cancel:   cancel,
		inbound:  make(chan inboundFrame, 16),
		readDone: make(chan struct{}),
	}
	go c.readLoop(newFrameReader(body))
	return c
}

func (c *rpcConn) setOnTerminate(fn func()) {
	c.onTerminate = fn
}

func (c *rpcConn) invokeTerminate() {
	c.onTerminateOnce.Do(func() {
		if c.onTerminate != nil {
			c.onTerminate()
		}
	})
}

func (c *rpcConn) readLoop(fr *frameReader) {
	defer close(c.readDone)
	defer close(c.inbound)
	abnormal := false
	defer func() {
		if abnormal {
			c.invokeTerminate()
		}
	}()
	for {
		hdr, payload, err := fr.readFrame()
		if err != nil {
			if err == io.EOF {
				c.pushInbound(inboundFrame{typ: frameHalfClose})
			} else {
				abnormal = true
				c.cancel()
			}
			return
		}
		if hdr.callID != singleCallID {
			continue
		}

		switch hdr.typ {
		case frameHalfClose:
			c.pushInbound(inboundFrame{typ: frameHalfClose})
			return
		case frameCancel:
			abnormal = true
			c.pushInbound(inboundFrame{
				err: status.Error(codes.Canceled, "call cancelled"),
			})
			c.cancel()
			return
		default:
			c.pushInbound(inboundFrame{typ: hdr.typ, payload: payload})
		}
	}
}

func (c *rpcConn) pushInbound(frame inboundFrame) {
	select {
	case c.inbound <- frame:
	case <-c.ctx.Done():
	}
}

func (c *rpcConn) Context() context.Context {
	return c.ctx
}

func (c *rpcConn) SetHeader(md metadata.MD) error {
	return nil
}

func (c *rpcConn) SendHeader(md metadata.MD) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sentHeader {
		return nil
	}
	return c.writeHeadersLocked(md)
}

func (c *rpcConn) SetTrailer(md metadata.MD) {}

func (c *rpcConn) RecvMsg(m any) error {
	for {
		c.mu.Lock()
		if c.requestReceived {
			body := c.requestBody
			c.requestBody = nil
			c.requestReceived = false
			c.mu.Unlock()
			return proto.Unmarshal(body, m.(proto.Message))
		}
		c.mu.Unlock()

		hdr, payload, err := c.readFrame()
		if err != nil {
			return err
		}

		c.mu.Lock()
		switch hdr.typ {
		case frameHeaders:
			hp, err := decodeJSON[headersPayload](payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.clientMetadata = metadataFromJSON(hp.Metadata)
			c.ctx = metadata.NewIncomingContext(c.ctx, c.clientMetadata)
			c.mu.Unlock()
		case frameMessage:
			body, err := decodeGrpcPayload(payload)
			c.mu.Unlock()
			if err != nil {
				return err
			}
			return proto.Unmarshal(body, m.(proto.Message))
		case frameHalfClose:
			c.recvClosed = true
			c.mu.Unlock()
			return io.EOF
		case frameCancel:
			c.mu.Unlock()
			return status.Error(codes.Canceled, "call cancelled")
		default:
			c.mu.Unlock()
		}
	}
}

func (c *rpcConn) SendMsg(m any) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.finished {
		return status.Error(codes.Internal, "grotto: call already finished")
	}

	msg, ok := m.(proto.Message)
	if !ok {
		return status.Error(codes.Internal, "grotto: message is not a proto.Message")
	}
	body, err := proto.Marshal(msg)
	if err != nil {
		return err
	}

	if !c.sentHeader {
		if err := c.writeHeadersLocked(nil); err != nil {
			return err
		}
	}

	if err := c.writeFrameLocked(frameMessage, wrapGrpcMessage(body)); err != nil {
		c.cancel()
		return err
	}
	return nil
}

func (c *rpcConn) consumeStreamHeaders() error {
	for {
		c.mu.Lock()
		if c.recvClosed {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		hdr, payload, err := c.readFrame()
		if err != nil {
			return err
		}

		c.mu.Lock()
		switch hdr.typ {
		case frameHeaders:
			hp, err := decodeJSON[headersPayload](payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.clientMetadata = metadataFromJSON(hp.Metadata)
			c.ctx = metadata.NewIncomingContext(c.ctx, c.clientMetadata)
			c.mu.Unlock()
			return nil
		case frameMessage:
			body, err := decodeGrpcPayload(payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.requestBody = body
			c.requestReceived = true
			c.mu.Unlock()
			return nil
		case frameHalfClose:
			c.recvClosed = true
			c.mu.Unlock()
			return nil
		case frameCancel:
			c.mu.Unlock()
			return status.Error(codes.Canceled, "call cancelled")
		default:
			c.mu.Unlock()
		}
	}
}

func (c *rpcConn) consumeServerStreamRequest() error {
	for {
		c.mu.Lock()
		if c.requestReceived {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		hdr, payload, err := c.readFrame()
		if err != nil {
			return err
		}

		c.mu.Lock()
		switch hdr.typ {
		case frameHeaders:
			hp, err := decodeJSON[headersPayload](payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.clientMetadata = metadataFromJSON(hp.Metadata)
			c.ctx = metadata.NewIncomingContext(c.ctx, c.clientMetadata)
			c.mu.Unlock()
		case frameMessage:
			body, err := decodeGrpcPayload(payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.requestBody = body
			c.requestReceived = true
			c.mu.Unlock()
			return nil
		case frameHalfClose:
			if c.requestReceived {
				c.recvClosed = true
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
			continue
		case frameCancel:
			c.mu.Unlock()
			return status.Error(codes.Canceled, "call cancelled")
		default:
			c.mu.Unlock()
		}
	}
}

func (c *rpcConn) consumeUnaryRequest() error {
	for {
		c.mu.Lock()
		if c.requestReceived {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()

		hdr, payload, err := c.readFrame()
		if err != nil {
			return err
		}

		c.mu.Lock()
		switch hdr.typ {
		case frameHeaders:
			hp, err := decodeJSON[headersPayload](payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.clientMetadata = metadataFromJSON(hp.Metadata)
			c.ctx = metadata.NewIncomingContext(c.ctx, c.clientMetadata)
			c.mu.Unlock()
		case frameMessage:
			body, err := decodeGrpcPayload(payload)
			if err != nil {
				c.mu.Unlock()
				return err
			}
			c.requestBody = body
			c.requestReceived = true
			c.mu.Unlock()
			return nil
		case frameHalfClose:
			if c.requestReceived {
				c.recvClosed = true
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
			continue
		case frameCancel:
			c.mu.Unlock()
			return status.Error(codes.Canceled, "call cancelled")
		default:
			c.mu.Unlock()
		}
	}
}

func (c *rpcConn) finishOK() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.finishLocked(codes.OK, nil)
}

func (c *rpcConn) finishError(err error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err == nil {
		return c.finishLocked(codes.OK, nil)
	}
	st, ok := status.FromError(err)
	if !ok {
		st = status.New(codes.Unknown, err.Error())
	}
	return c.finishLocked(st.Code(), st)
}

func (c *rpcConn) finishLocked(code codes.Code, st *status.Status) error {
	if c.finished {
		return nil
	}
	c.finished = true

	details := ""
	if st != nil {
		details = st.Message()
	}
	tp := trailersPayload{
		Code:     int(code),
		Details:  details,
		Metadata: map[string]any{},
	}
	payload, err := encodeJSON(tp)
	if err != nil {
		c.cancel()
		return err
	}
	if err := c.writeFrameLocked(frameTrailers, payload); err != nil {
		c.cancel()
		return err
	}
	c.cancel()
	return nil
}

func (c *rpcConn) readFrame() (frameHeader, []byte, error) {
	select {
	case <-c.ctx.Done():
		return frameHeader{}, nil, c.ctx.Err()
	case frame, ok := <-c.inbound:
		if !ok {
			return frameHeader{}, nil, io.EOF
		}
		if frame.err != nil {
			return frameHeader{}, nil, frame.err
		}
		return frameHeader{callID: singleCallID, typ: frame.typ}, frame.payload, nil
	}
}

func (c *rpcConn) writeFrameLocked(typ uint8, payload []byte) error {
	return c.writer.writeFrame(singleCallID, typ, payload)
}

func (c *rpcConn) writeHeadersLocked(md metadata.MD) error {
	meta := map[string]any{}
	if md != nil {
		meta = mdToJSON(md)
	}
	payload, err := encodeJSON(headersPayload{Metadata: meta})
	if err != nil {
		return err
	}
	if err := c.writeFrameLocked(frameHeaders, payload); err != nil {
		return err
	}
	c.sentHeader = true
	return nil
}

func metadataFromJSON(raw map[string]any) metadata.MD {
	md := metadata.MD{}
	if raw == nil {
		return md
	}
	for k, v := range raw {
		switch val := v.(type) {
		case string:
			md.Append(k, val)
		case []any:
			for _, item := range val {
				if s, ok := item.(string); ok {
					md.Append(k, s)
				}
			}
		}
	}
	return md
}

func mdToJSON(md metadata.MD) map[string]any {
	out := map[string]any{}
	for k, vals := range md {
		if len(vals) == 1 {
			out[k] = vals[0]
		} else {
			items := make([]any, len(vals))
			for i, v := range vals {
				items[i] = v
			}
			out[k] = items
		}
	}
	return out
}

var errNotProto = errors.New("grotto: not a proto.Message")
