package grottoserver

import (
	"bytes"
	"testing"
)

func TestParseMethodPath(t *testing.T) {
	service, method, ok := ParseMethodPath("/ping.PingService/Ping")
	if !ok || service != "ping.PingService" || method != "Ping" {
		t.Fatalf("ParseMethodPath() = %q, %q, %v", service, method, ok)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	payload := []byte("hello")
	frame := encodeFrame(singleCallID, frameMessage, payload, 0)
	hdr, got, err := decodeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if hdr.callID != singleCallID || hdr.typ != frameMessage {
		t.Fatalf("header = %+v", hdr)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("payload = %q", got)
	}
}

func TestDecodeGrpcPayload(t *testing.T) {
	raw := []byte{0x0a, 0x05, 'h', 'e', 'l', 'l', 'o'}
	if body, err := decodeGrpcPayload(raw); err != nil || !bytes.Equal(body, raw) {
		t.Fatalf("raw decode = %q, %v", body, err)
	}

	wrapped := wrapGrpcMessage(raw)
	body, err := decodeGrpcPayload(wrapped)
	if err != nil || !bytes.Equal(body, raw) {
		t.Fatalf("wrapped decode = %q, %v", body, err)
	}
}
