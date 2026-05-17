package grottoserver

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type rpcConn struct {
	ws *websocket.Conn
	mu sync.Mutex

	ctx            context.Context
	clientMetadata metadata.MD
	requestBody    []byte
	recvClosed     bool
	sentHeader     bool
	finished       bool
}

func newRPCConn(ws *websocket.Conn) *rpcConn {
	return &rpcConn{
		ws:  ws,
		ctx: context.Background(),
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
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.recvClosed {
		return io.EOF
	}

	if len(c.requestBody) > 0 {
		body := c.requestBody
		c.requestBody = nil
		return proto.Unmarshal(body, m.(proto.Message))
	}

	for {
		typ, payload, err := c.readFrameLocked()
		if err != nil {
			return err
		}
		if typ.callID != singleCallID {
			continue
		}
		switch typ.typ {
		case frameHeaders:
			hp, err := decodeJSON[headersPayload](payload)
			if err != nil {
				return err
			}
			c.clientMetadata = metadataFromJSON(hp.Metadata)
			c.ctx = metadata.NewIncomingContext(c.ctx, c.clientMetadata)
		case frameMessage:
			body, err := decodeGrpcPayload(payload)
			if err != nil {
				return err
			}
			return proto.Unmarshal(body, m.(proto.Message))
		case frameHalfClose:
			c.recvClosed = true
			return io.EOF
		case frameCancel:
			return status.Error(codes.Canceled, "call cancelled")
		default:
			continue
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

	return c.writeFrameLocked(frameMessage, wrapGrpcMessage(body))
}

func (c *rpcConn) consumeUnaryRequest() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for !c.recvClosed {
		hdr, payload, err := c.readFrameLocked()
		if err != nil {
			return err
		}
		if hdr.callID != singleCallID {
			continue
		}
		switch hdr.typ {
		case frameHeaders:
			hp, err := decodeJSON[headersPayload](payload)
			if err != nil {
				return err
			}
			c.clientMetadata = metadataFromJSON(hp.Metadata)
			c.ctx = metadata.NewIncomingContext(c.ctx, c.clientMetadata)
		case frameMessage:
			body, err := decodeGrpcPayload(payload)
			if err != nil {
				return err
			}
			c.requestBody = body
		case frameHalfClose:
			c.recvClosed = true
			return nil
		case frameCancel:
			return status.Error(codes.Canceled, "call cancelled")
		}
	}
	return nil
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
		return err
	}
	if err := c.writeFrameLocked(frameTrailers, payload); err != nil {
		return err
	}
	return c.ws.Close()
}

func (c *rpcConn) readFrameLocked() (frameHeader, []byte, error) {
	_, data, err := c.ws.ReadMessage()
	if err != nil {
		return frameHeader{}, nil, err
	}
	return decodeFrame(data)
}

func (c *rpcConn) writeFrameLocked(typ uint8, payload []byte) error {
	return c.ws.WriteMessage(websocket.BinaryMessage, encodeFrame(singleCallID, typ, payload, 0))
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

var errNotProto = errors.New("grotto: not a proto message")
