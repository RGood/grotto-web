package grottoserver

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
)

const grottoMagic uint32 = 0x47525443 // "GRTC"

const (
	frameHeaders   = 1
	frameMessage   = 2
	frameHalfClose = 3
	frameTrailers  = 4
	frameCancel    = 5
)

const singleCallID uint32 = 1
const headerSize = 16

type frameHeader struct {
	callID uint32
	typ    uint8
	flags  uint8
	length uint32
}

func encodeFrame(callID uint32, typ uint8, payload []byte, flags uint8) []byte {
	out := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(out[0:], grottoMagic)
	binary.BigEndian.PutUint32(out[4:], callID)
	out[8] = typ
	out[9] = flags
	binary.BigEndian.PutUint16(out[10:], 0)
	binary.BigEndian.PutUint32(out[12:], uint32(len(payload)))
	copy(out[headerSize:], payload)
	return out
}

func decodeFrame(data []byte) (frameHeader, []byte, error) {
	if len(data) < headerSize {
		return frameHeader{}, nil, errors.New("grotto: frame too short")
	}
	if binary.BigEndian.Uint32(data[0:]) != grottoMagic {
		return frameHeader{}, nil, fmt.Errorf("grotto: invalid magic 0x%x", binary.BigEndian.Uint32(data[0:]))
	}
	hdr := frameHeader{
		callID: binary.BigEndian.Uint32(data[4:]),
		typ:    data[8],
		flags:  data[9],
		length: binary.BigEndian.Uint32(data[12:]),
	}
	if headerSize+int(hdr.length) > len(data) {
		return frameHeader{}, nil, errors.New("grotto: truncated frame payload")
	}
	payload := data[headerSize : headerSize+int(hdr.length)]
	return hdr, payload, nil
}

func encodeJSON(v any) ([]byte, error) {
	return json.Marshal(v)
}

func decodeJSON[T any](payload []byte) (T, error) {
	var out T
	if len(payload) == 0 {
		return out, nil
	}
	err := json.Unmarshal(payload, &out)
	return out, err
}

type headersPayload struct {
	Metadata map[string]any `json:"metadata"`
}

type trailersPayload struct {
	Code     int            `json:"code"`
	Details  string         `json:"details"`
	Metadata map[string]any `json:"metadata"`
}

func unwrapGrpcMessage(frame []byte) ([]byte, error) {
	if len(frame) < 5 {
		return nil, errors.New("grotto: invalid gRPC message frame")
	}
	length := binary.BigEndian.Uint32(frame[1:5])
	if 5+int(length) > len(frame) {
		return nil, errors.New("grotto: truncated gRPC message frame")
	}
	return frame[5 : 5+length], nil
}

// decodeGrpcPayload accepts either a gRPC length-prefixed frame or a raw protobuf blob.
func decodeGrpcPayload(frame []byte) ([]byte, error) {
	if body, err := unwrapGrpcMessage(frame); err == nil {
		return body, nil
	}
	return frame, nil
}

func wrapGrpcMessage(payload []byte) []byte {
	out := make([]byte, 5+len(payload))
	out[0] = 0
	binary.BigEndian.PutUint32(out[1:], uint32(len(payload)))
	copy(out[5:], payload)
	return out
}
