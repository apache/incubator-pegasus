/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package util

import (
	"encoding/hex"
	"fmt"
)

type asciiHexEncoder struct {
}

func (*asciiHexEncoder) EncodeAll(s string) ([]byte, error) {
	var bs []byte

	for i := 0; i < len(s); i++ {
		if s[i] == '\\' {
			if i+1 >= len(s) {
				return nil, fmt.Errorf("invalid asciihex string: %s", s)
			}

			i++
			switch s[i] {
			case 'n':
				bs = append(bs, '\n')
			case 'r':
				bs = append(bs, '\r')
			case 't':
				bs = append(bs, '\t')
			case '"':
				bs = append(bs, '"')
			case '\'':
				bs = append(bs, '\'')
			case '\\':
				bs = append(bs, '\\')
			case 'x', 'X':
				if i+2 >= len(s) {
					return nil, fmt.Errorf("invalid asciihex string: %s", s)
				}
				digit1 := s[i+1]
				digit2 := s[i+2]
				i += 2

				var hexStr string
				hexStr += string(digit1) + string(digit2)
				bytes, err := hex.DecodeString(hexStr)
				if err != nil {
					return nil, err
				}
				if len(bytes) != 1 {
					panic("two hex digits should be 1 byte")
				}

				bs = append(bs, bytes[0])
			}
		} else if s[i] >= 32 && s[i] <= 126 {
			// valid ascii character
			bs = append(bs, s[i])
		} else {
			return nil, fmt.Errorf("invalid asciihex string: %s", s)
		}
	}
	return bs, nil
}

func (*asciiHexEncoder) DecodeAll(bytes []byte) (string, error) {
	var s string
	for _, c := range bytes {
		hexByte := hex.EncodeToString([]byte{c})
		s += "\\x" + hexByte
	}
	return s, nil
}

func (*asciiHexEncoder) String() string {
	return "ASCIIHEX"
}
