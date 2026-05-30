package grottoserver

import (
	"encoding/binary"
	"io"
	"net/http"
)

const grottoContentType = "application/vnd.grotto+grtc"

// connectionClose forces one TCP connection per RPC so bidi's second POST is not
// issued while the first response body is still streaming on a keep-alive socket.
const connectionClose = "close"

func setGrottoResponseHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", grottoContentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", connectionClose)
}

type frameReader struct {
	r   io.Reader
	buf []byte
}

func newFrameReader(r io.Reader) *frameReader {
	return &frameReader{r: r}
}

func (fr *frameReader) readFrame() (frameHeader, []byte, error) {
	for {
		if err := fr.fill(headerSize); err != nil {
			return frameHeader{}, nil, err
		}
		length := int(binary.BigEndian.Uint32(fr.buf[12:16]))
		total := headerSize + length
		if err := fr.fill(total); err != nil {
			return frameHeader{}, nil, err
		}
		frame := fr.buf[:total]
		fr.buf = fr.buf[total:]
		return decodeFrame(frame)
	}
}

func (fr *frameReader) fill(n int) error {
	for len(fr.buf) < n {
		chunk := make([]byte, 4096)
		read, err := fr.r.Read(chunk)
		if read > 0 {
			fr.buf = append(fr.buf, chunk[:read]...)
		}
		if err != nil {
			if err == io.EOF && len(fr.buf) >= n {
				return nil
			}
			if err == io.EOF {
				return io.EOF
			}
			return err
		}
		if read == 0 {
			return io.ErrUnexpectedEOF
		}
	}
	return nil
}

type frameWriter struct {
	w       io.Writer
	flusher http.Flusher
}

func newFrameWriter(w http.ResponseWriter) *frameWriter {
	fw := &frameWriter{w: w}
	if f, ok := w.(http.Flusher); ok {
		fw.flusher = f
	}
	return fw
}

func (fw *frameWriter) writeFrame(callID uint32, typ uint8, payload []byte) error {
	if _, err := fw.w.Write(encodeFrame(callID, typ, payload, 0)); err != nil {
		return err
	}
	if fw.flusher != nil {
		fw.flusher.Flush()
	}
	return nil
}
