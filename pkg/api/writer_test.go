package resp

import (
	"bytes"
	"strings"
	"testing"
)

func TestWriteSimpleString(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteValue(SimpleString("OK"))
	if buf.String() != "+OK\r\n" {
		t.Fatalf("expected '+OK\\r\\n', got %q", buf.String())
	}
}

func TestWriteError(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteValue(Error("ERR something"))
	if buf.String() != "-ERR something\r\n" {
		t.Fatalf("expected '-ERR something\\r\\n', got %q", buf.String())
	}
}

func TestWriteInteger(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteValue(Integer(42))
	if buf.String() != ":42\r\n" {
		t.Fatalf("expected ':42\\r\\n', got %q", buf.String())
	}
}

func TestWriteBulkString(t *testing.T) {
	tests := []struct {
		name  string
		value BulkString
		want  string
	}{
		{"normal", NewBulkString("hello"), "$5\r\nhello\r\n"},
		{"empty", NewBulkString(""), "$0\r\n\r\n"},
		{"null", NullBulkString(), "$-1\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			w.WriteValue(tt.value)
			if buf.String() != tt.want {
				t.Fatalf("expected %q, got %q", tt.want, buf.String())
			}
		})
	}
}

func TestWriteArray(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteValue(Array{
		NewBulkString("SET"),
		NewBulkString("key"),
		NewBulkString("value"),
	})
	want := "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"
	if buf.String() != want {
		t.Fatalf("expected %q, got %q", want, buf.String())
	}
}

func TestRoundTrip(t *testing.T) {
	inputs := []string{
		"+OK\r\n",
		"-ERR bad\r\n",
		":99\r\n",
		"$5\r\nhello\r\n",
		"$-1\r\n",
		"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
	}
	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			// Read
			r := NewReader(strings.NewReader(input))
			v, err := r.ReadValue()
			if err != nil {
				t.Fatalf("read error: %v", err)
			}
			// Write
			var buf bytes.Buffer
			w := NewWriter(&buf)
			w.WriteValue(v)
			if buf.String() != input {
				t.Fatalf("round-trip: expected %q, got %q", input, buf.String())
			}
		})
	}
}
