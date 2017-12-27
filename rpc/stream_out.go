// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package rpc

import (
	"io"
)

type RpcWriteStream struct {
	writer io.Writer
}

// NewRpcWriteStream always receives a *net.TcpConn as `writer`, except in
// testing it can accept a buffer as the fake writer.
func NewRpcWriteStream(writer io.Writer) *RpcWriteStream {
	return &RpcWriteStream{
		writer: writer,
	}
}

// invoke an asynchronous write for message.
func (s *RpcWriteStream) Write(msgBytes []byte) error {
	_, err := s.writer.Write(msgBytes)
	return err
}
