package resp

import (
	"bufio"
	"io"
)

// Writer writes RESP-encoded values to an io.Writer.
type Writer struct {
	bw *bufio.Writer
}

// NewWriter wraps an io.Writer into a RESP writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriter(w)}
}

// Write writes a single RESP value to the buffer (does not flush).
func (w *Writer) Write(v Value) {
	w.bw.Write(v.Resp()) // error ignored — bufio.Writer errors surface on Flush
}

// Flush writes any buffered data to the underlying io.Writer.
func (w *Writer) Flush() error {
	return w.bw.Flush()
}

// WriteValue is a convenience that writes and flushes in one call.
func (w *Writer) WriteValue(v Value) error {
	w.Write(v)
	return w.Flush()
}
