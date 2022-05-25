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

package rpc

import (
	"bytes"
	"io"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestRpcWriteStream_WriteAndFlush(t *testing.T) {
	defer leaktest.Check(t)()

	expectedBytes := []byte("hello world")
	buf := bytes.NewBuffer(make([]byte, 0))
	stream := NewWriteStream(buf)

	// call write on each byte
	for _, b := range expectedBytes {
		err := stream.Write([]byte{b})
		assert.Nil(t, err)
	}

	assert.Equal(t, expectedBytes, buf.Bytes())
}

func TestRpcReadStream_Read(t *testing.T) {
	defer leaktest.Check(t)()

	buf := bytes.NewBuffer(make([]byte, 0))

	in := NewReadStream(buf)

	expectedBytes := []byte("hello world")
	out := NewWriteStream(buf)
	for _, b := range expectedBytes {
		err := out.Write([]byte{b})
		assert.Nil(t, err)
	}

	actualBytes := make([]byte, 0)
	for {
		data, err := in.Next(len(expectedBytes))
		if err == io.EOF {
			break
		}
		assert.NotNil(t, data)
		assert.Nil(t, err)

		actualBytes = append(actualBytes, data...)
	}

	assert.Equal(t, expectedBytes, actualBytes)
}
