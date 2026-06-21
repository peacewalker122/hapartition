package resp

import "fmt"

// Value is a RESP value that can be serialized to the Redis wire protocol.
type Value interface {
	// Resp returns the RESP-encoded byte representation.
	Resp() []byte
}

// SimpleString represents a RESP Simple String (+OK\r\n).
type SimpleString string

func (s SimpleString) Resp() []byte {
	return []byte(fmt.Sprintf("+%s\r\n", string(s)))
}

// Error represents a RESP Error (-ERR ...\r\n).
type Error string

func (e Error) Resp() []byte {
	return []byte(fmt.Sprintf("-%s\r\n", string(e)))
}

// Integer represents a RESP Integer (:42\r\n).
type Integer int

func (i Integer) Resp() []byte {
	return []byte(fmt.Sprintf(":%d\r\n", int(i)))
}

// BulkString represents a RESP Bulk String ($5\r\nhello\r\n).
// A nil BulkString encodes as "$-1\r\n".
type BulkString struct {
	Data []byte // nil means null bulk string
}

// NewBulkString creates a BulkString from a string.
func NewBulkString(s string) BulkString {
	return BulkString{Data: []byte(s)}
}

// NullBulkString returns a representation of a null bulk string.
func NullBulkString() BulkString {
	return BulkString{Data: nil}
}

func (b BulkString) Resp() []byte {
	if b.Data == nil {
		return []byte("$-1\r\n")
	}
	return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(b.Data), string(b.Data)))
}

// Array represents a RESP Array (*2\r\n...).
type Array []Value

func (a Array) Resp() []byte {
	b := []byte(fmt.Sprintf("*%d\r\n", len(a)))
	for _, v := range a {
		b = append(b, v.Resp()...)
	}
	return b
}

// OK is a convenience constant for +OK\r\n.
var OK Value = SimpleString("OK")

// PONG is a convenience constant for +PONG\r\n.
var PONG Value = SimpleString("PONG")
