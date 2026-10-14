package resp

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
)

const (
	maxBulkStringLength = 64 << 20
	maxArrayLength      = 1 << 20
	maxLineLength       = 64 << 10
	maxArrayDepth       = 32
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
	return rd.readValue(0)
}

func (rd *Reader) readValue(depth int) (Value, error) {
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
		if depth >= maxArrayDepth {
			return nil, fmt.Errorf("resp: array nesting exceeds %d", maxArrayDepth)
		}
		arr, err := rd.readArray(depth + 1)
		if err != nil {
			return nil, err
		}
		if arr == nil {
			return nil, nil
		}
		return arr, nil
	default:
		rd.br.UnreadByte()
		return rd.readInline()
	}
}

func (rd *Reader) readLine() (string, error) {
	var line []byte
	for {
		part, err := rd.br.ReadSlice('\n')
		if len(line)+len(part) > maxLineLength {
			return "", fmt.Errorf("resp: line exceeds %d bytes", maxLineLength)
		}
		line = append(line, part...)
		if err == nil {
			break
		}
		if err != bufio.ErrBufferFull {
			return "", err
		}
	}
	if len(line) < 2 || line[len(line)-2] != '\r' {
		return "", errors.New("resp: missing CRLF terminator")
	}
	return string(line[:len(line)-2]), nil
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
	if n < 0 {
		return NullBulkString(), fmt.Errorf("resp: invalid negative bulk string length %d", n)
	}
	if n > maxBulkStringLength {
		return NullBulkString(), fmt.Errorf("resp: bulk string length %d exceeds %d", n, maxBulkStringLength)
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

func (rd *Reader) readArray(depth int) (Array, error) {
	s, err := rd.readLine()
	if err != nil {
		return nil, err
	}
	if s == "-1" {
		return nil, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil {
		return nil, fmt.Errorf("resp: invalid array length %q: %w", s, err)
	}
	if n < 0 {
		return nil, fmt.Errorf("resp: invalid negative array length %d", n)
	}
	if n > maxArrayLength {
		return nil, fmt.Errorf("resp: array length %d exceeds %d", n, maxArrayLength)
	}
	arr := make(Array, n)
	for i := range n {
		v, err := rd.readValue(depth)
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
