package middleware

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"net/http"
)

type boundedResponseWriter struct {
	http.ResponseWriter
	limit       int64
	header      http.Header
	status      int
	wroteHeader bool
	overflow    bool
	streaming   bool
	bodyBuffer  bytes.Buffer
	writeErr    error
}

func newBoundedResponseWriter(w http.ResponseWriter, limit int64) *boundedResponseWriter {
	return &boundedResponseWriter{
		ResponseWriter: w,
		limit:          limit,
		header:         make(http.Header),
	}
}

func (w *boundedResponseWriter) Header() http.Header {
	if w.streaming {
		return w.ResponseWriter.Header()
	}
	return w.header
}

func (w *boundedResponseWriter) WriteHeader(status int) {
	if w.wroteHeader {
		return
	}
	w.status = status
	w.wroteHeader = true
}

func (w *boundedResponseWriter) Write(data []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.streaming {
		return w.ResponseWriter.Write(data)
	}
	if w.capture(data) {
		return len(data), nil
	}
	w.startStreaming(data)
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	return len(data), nil
}

func (w *boundedResponseWriter) Flush() {
	if !w.streaming {
		w.startStreaming(nil)
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *boundedResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, errors.New("underlying response writer does not support hijacking")
	}
	return hijacker.Hijack()
}

func (w *boundedResponseWriter) Push(target string, opts *http.PushOptions) error {
	pusher, ok := w.ResponseWriter.(http.Pusher)
	if !ok {
		return http.ErrNotSupported
	}
	return pusher.Push(target, opts)
}

func (w *boundedResponseWriter) send() error {
	if w.streaming {
		return w.writeErr
	}
	w.streaming = true
	copyHeader(w.ResponseWriter.Header(), w.header)
	w.ResponseWriter.WriteHeader(w.statusCode())
	if w.bodyBuffer.Len() == 0 {
		return nil
	}
	_, err := w.ResponseWriter.Write(w.bodyBuffer.Bytes())
	return err
}

func (w *boundedResponseWriter) statusCode() int {
	if w.status == 0 {
		return http.StatusOK
	}
	return w.status
}

func (w *boundedResponseWriter) body() []byte {
	if w.overflow {
		return nil
	}
	return append([]byte(nil), w.bodyBuffer.Bytes()...)
}

func (w *boundedResponseWriter) replayable() bool {
	return !w.overflow
}

func (w *boundedResponseWriter) sent() bool {
	return w.streaming
}

func (w *boundedResponseWriter) capture(data []byte) bool {
	if w.overflow {
		return false
	}
	remaining := w.limit - int64(w.bodyBuffer.Len())
	if remaining < int64(len(data)) {
		w.overflow = true
		return false
	}
	_, _ = w.bodyBuffer.Write(data)
	return true
}

func (w *boundedResponseWriter) startStreaming(first []byte) {
	if w.streaming {
		return
	}
	w.streaming = true
	w.wroteHeader = true
	w.status = w.statusCode()
	copyHeader(w.ResponseWriter.Header(), w.header)
	w.ResponseWriter.WriteHeader(w.status)
	if w.bodyBuffer.Len() > 0 {
		if _, err := io.Copy(w.ResponseWriter, bytes.NewReader(w.bodyBuffer.Bytes())); err != nil {
			w.writeErr = err
			return
		}
		w.bodyBuffer.Reset()
	}
	if len(first) > 0 {
		_, w.writeErr = w.ResponseWriter.Write(first)
	}
}

func copyHeader(dst, src http.Header) {
	for key, values := range src {
		dst.Del(key)
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}
