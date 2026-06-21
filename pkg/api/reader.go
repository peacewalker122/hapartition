package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

// Reader reads RESP-encoded values from an io.Reader.
type Reader struct {
	br *bufio.Reader
}

// NewReader wraps an io.Reader into a RESP reader.
func NewReader(r io.Reader) *Reader {
	return &Reader{br: bufio.NewReader(r)}
}

// ReadValue reads one complete RESP value from the stream.
func (rd *Reader) ReadValue() (Value, error) {
	b, err := rd.br.ReadByte()
	if err != nil {
		return nil, err
	}
	switch b {
	case '+':
		return rd.readSimpleString()
	case '-':
		return rd.readError()
	case ':':
		return rd.readInteger()
	case '$':
		return rd.readBulkString()
	case '*':
		arr, err := rd.readArray()
		if err != nil {
			return nil, err
		}
		if arr == nil {
			return nil, nil // null array → nil interface
		}
		return arr, nil
	default:
		// Inline command — put the byte back and parse as inline
		rd.br.UnreadByte()
		return rd.readInline()
	}
}

func (rd *Reader) readLine() (string, error) {
	s, err := rd.br.ReadString('\n')
	if err != nil {
		return "", err
	}
	if len(s) < 2 || s[len(s)-2] != '\r' {
		return "", errors.New("resp: missing CRLF terminator")
	}
	return s[:len(s)-2], nil
}

func (rd *Reader) readSimpleString() (SimpleString, error) {
	s, err := rd.readLine()
	if err != nil {
		return "", err
	}
	return SimpleString(s), nil
}

func (rd *Reader) readError() (Error, error) {
	s, err := rd.readLine()
	if err != nil {
		return "", err
	}
	return Error(s), nil
}

func (rd *Reader) readInteger() (Integer, error) {
	s, err := rd.readLine()
	if err != nil {
		return 0, err
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("resp: invalid integer %q: %w", s, err)
	}
	return Integer(n), nil
}

func (rd *Reader) readBulkString() (BulkString, error) {
	s, err := rd.readLine()
	if err != nil {
		return NullBulkString(), err
	}
	if s == "-1" {
		return NullBulkString(), nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return NullBulkString(), fmt.Errorf("resp: invalid bulk string length %q: %w", s, err)
	}

	buf := make([]byte, n)
	_, err = io.ReadFull(rd.br, buf)
	if err != nil {
		return NullBulkString(), fmt.Errorf("resp: short read for bulk string: %w", err)
	}

	// consume trailing \r\n
	cr := make([]byte, 1)
	_, err = io.ReadFull(rd.br, cr)
	if err != nil {
		return NullBulkString(), fmt.Errorf("resp: missing CR after bulk string: %w", err)
	}
	if cr[0] != '\r' {
		return NullBulkString(), fmt.Errorf("resp: expected CR after bulk string, got %q", cr[0])
	}
	lf := make([]byte, 1)
	_, err = io.ReadFull(rd.br, lf)
	if err != nil {
		return NullBulkString(), fmt.Errorf("resp: missing LF after bulk string: %w", err)
	}
	if lf[0] != '\n' {
		return NullBulkString(), fmt.Errorf("resp: expected LF after bulk string, got %q", lf[0])
	}

	return BulkString{Data: buf}, nil
}

func (rd *Reader) readArray() (Array, error) {
	s, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if s == "-1" {
		return nil, nil // null array
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return nil, fmt.Errorf("resp: invalid array length %q: %w", s, err)
	}
	arr := make(Array, n)
	for i := 0; i < n; i++ {
		v, err := rd.ReadValue()
		if err != nil {
			return nil, fmt.Errorf("resp: array element %d: %w", i, err)
		}
		arr[i] = v
	}
	return arr, nil
}

// readInline handles inline (non-RESP) commands like "SET key value\r\n".
// redis-cli sends inline commands on initial connect in many modes.
func (rd *Reader) readInline() (Array, error) {
	s, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	parts := strings.Fields(s)
	arr := make(Array, len(parts))
	for i, p := range parts {
		arr[i] = NewBulkString(p)
	}
	return arr, nil
}
