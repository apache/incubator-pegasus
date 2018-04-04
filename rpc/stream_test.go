// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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
