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
