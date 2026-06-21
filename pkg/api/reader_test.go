package resp

import (
	"strings"
	"testing"
)

func TestReadSimpleString(t *testing.T) {
	r := NewReader(strings.NewReader("+OK\r\n"))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	s, ok := v.(SimpleString)
	if !ok {
		t.Fatalf("expected SimpleString, got %T", v)
	}
	if string(s) != "OK" {
		t.Fatalf("expected 'OK', got %q", string(s))
	}
}

func TestReadError(t *testing.T) {
	r := NewReader(strings.NewReader("-ERR wrong type\r\n"))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	e, ok := v.(Error)
	if !ok {
		t.Fatalf("expected Error, got %T", v)
	}
	if string(e) != "ERR wrong type" {
		t.Fatalf("expected 'ERR wrong type', got %q", string(e))
	}
}

func TestReadInteger(t *testing.T) {
	r := NewReader(strings.NewReader(":42\r\n"))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	i, ok := v.(Integer)
	if !ok {
		t.Fatalf("expected Integer, got %T", v)
	}
	if int(i) != 42 {
		t.Fatalf("expected 42, got %d", int(i))
	}
}

func TestReadBulkString(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
		isNil bool
	}{
		{"normal", "$5\r\nhello\r\n", "hello", false},
		{"empty", "$0\r\n\r\n", "", false},
		{"null", "$-1\r\n", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.input))
			v, err := r.ReadValue()
			if err != nil {
				t.Fatal(err)
			}
			b, ok := v.(BulkString)
			if !ok {
				t.Fatalf("expected BulkString, got %T", v)
			}
			if tt.isNil {
				if b.Data != nil {
					t.Fatalf("expected nil data, got %q", string(b.Data))
				}
			} else {
				if string(b.Data) != tt.want {
					t.Fatalf("expected %q, got %q", tt.want, string(b.Data))
				}
			}
		})
	}
}

func TestReadArray(t *testing.T) {
	r := NewReader(strings.NewReader("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n"))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.(Array)
	if !ok {
		t.Fatalf("expected Array, got %T", v)
	}
	if len(arr) != 2 {
		t.Fatalf("expected 2 elements, got %d", len(arr))
	}
	cmd, ok := arr[0].(BulkString)
	if !ok || string(cmd.Data) != "GET" {
		t.Fatalf("expected BulkString 'GET', got %v", arr[0])
	}
	key, ok := arr[1].(BulkString)
	if !ok || string(key.Data) != "key" {
		t.Fatalf("expected BulkString 'key', got %v", arr[1])
	}
}

func TestReadNullArray(t *testing.T) {
	r := NewReader(strings.NewReader("*-1\r\n"))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if v != nil {
		t.Fatalf("expected nil for null array, got %T", v)
	}
}

func TestReadInlineCommand(t *testing.T) {
	r := NewReader(strings.NewReader("SET key value\r\n"))
	v, err := r.ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	arr, ok := v.(Array)
	if !ok {
		t.Fatalf("expected Array for inline command, got %T", v)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(arr))
	}
	parts := make([]string, len(arr))
	for i, p := range arr {
		b, ok := p.(BulkString)
		if !ok {
			t.Fatalf("element %d: expected BulkString, got %T", i, p)
		}
		parts[i] = string(b.Data)
	}
	if parts[0] != "SET" || parts[1] != "key" || parts[2] != "value" {
		t.Fatalf("expected [SET key value], got %v", parts)
	}
}
