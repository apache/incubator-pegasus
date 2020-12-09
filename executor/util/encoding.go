package util

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"
)

type Encoder interface {
	// encode string into a bytes
	EncodeAll(string) ([]byte, error)

	// decode from bytes to string
	DecodeAll([]byte) (string, error)

	// String returns name of the encoding.
	String() string
}

type utf8Encoder struct {
}

func (*utf8Encoder) EncodeAll(s string) ([]byte, error) {
	if !utf8.ValidString(s) {
		return nil, errors.New("invalid utf8 string")
	}
	return []byte(s), nil // go uses utf8 by default.
}

func (*utf8Encoder) DecodeAll(s []byte) (string, error) {
	if !utf8.Valid(s) {
		return "", errors.New("invalid utf8 bytes")
	}
	return string(s), nil
}

func (*utf8Encoder) String() string {
	return "UTF8"
}

type int32Encoder struct {
}

func (*int32Encoder) EncodeAll(s string) ([]byte, error) {
	i, err := strconv.ParseInt(s, 10, 32 /*bits*/)
	if err != nil {
		return nil, errors.New("invalid INT32")
	}
	value := make([]byte, 4 /*bytes*/)
	binary.BigEndian.PutUint32(value, uint32(i))
	return value, nil
}

func (*int32Encoder) DecodeAll(s []byte) (string, error) {
	if len(s) != 4 {
		return "", fmt.Errorf("bytes is not a valid INT32")
	}
	i := binary.BigEndian.Uint32(s)
	return fmt.Sprint(int32(i)), nil
}

func (*int32Encoder) String() string {
	return "INT32"
}

type int64Encoder struct {
}

func (*int64Encoder) EncodeAll(s string) ([]byte, error) {
	i, err := strconv.ParseInt(s, 10, 64 /*bits*/)
	if err != nil {
		return nil, errors.New("invalid INT64")
	}
	value := make([]byte, 8 /*bytes*/)
	binary.BigEndian.PutUint64(value, uint64(i))
	return value, nil
}

func (*int64Encoder) DecodeAll(s []byte) (string, error) {
	if len(s) != 8 {
		return "", fmt.Errorf("bytes is not a valid INT64")
	}
	i := binary.BigEndian.Uint64(s)
	return fmt.Sprint(int64(i)), nil
}

func (*int64Encoder) String() string {
	return "INT64"
}

type goBytesEncoder struct {
}

func (*goBytesEncoder) EncodeAll(s string) ([]byte, error) {
	bytesInStrList := strings.Split(s, " ")

	value := make([]byte, len(bytesInStrList))
	for i, byteStr := range bytesInStrList {
		b, err := strconv.Atoi(byteStr)
		if err != nil || b > 255 || b < 0 { // byte ranges from [0, 255]
			return nil, fmt.Errorf("invalid go byte \"%s\"", byteStr)
		}
		value[i] = byte(b)
	}

	return value, nil
}

func (*goBytesEncoder) DecodeAll(bytes []byte) (string, error) {
	s := make([]string, len(bytes))
	for i, c := range bytes {
		s[i] = fmt.Sprint(uint8(c))
	}
	return strings.Join(s, ","), nil
}

func (*goBytesEncoder) String() string {
	return "BYTES"
}

// NewEncoder returns nil if the given name is invalid.
func NewEncoder(name string) Encoder {
	name = strings.ToLower(name)
	switch name {
	case "utf8", "utf-8":
		return &utf8Encoder{}
	case "int32":
		return &int32Encoder{}
	case "int64":
		return &int64Encoder{}
	case "bytes":
		return &goBytesEncoder{}
	case "javabytes":
		return &javaBytesEncoder{}
	case "asciihex":
		return &asciiHexEncoder{}
	// TODO(wutao): support hex array, such as 0x37, 0xFF, ...
	default:
		return nil
	}
}
