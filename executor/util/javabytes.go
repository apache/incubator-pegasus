package util

import (
	"fmt"
	"strconv"
	"strings"
)

// javabytes is like '0,0,101,12,0,1,8,-1,-1,-1', delimited by ',' and each byte ranges from [-128, 127]
type javaBytesEncoder struct {
}

func (*javaBytesEncoder) EncodeAll(s string) ([]byte, error) {
	bytesInStrList := strings.Split(s, ",")

	value := make([]byte, len(bytesInStrList))
	for i, byteStr := range bytesInStrList {
		b, err := strconv.Atoi(byteStr)
		if err != nil || b > 127 || b < -128 { // byte ranges from [-128, 127]
			return nil, fmt.Errorf("invalid java byte \"%s\"", byteStr)
		}
		value[i] = byte(b)
	}

	return value, nil
}

func (*javaBytesEncoder) DecodeAll(bytes []byte) (string, error) {
	s := make([]string, len(bytes))
	for i, c := range bytes {
		s[i] = fmt.Sprint(int8(c))
	}
	return strings.Join(s, ","), nil
}

func (*javaBytesEncoder) String() string {
	return "JAVABYTES"
}
