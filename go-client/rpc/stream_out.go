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
	"io"
)

// low-level rpc writer.
type WriteStream struct {
	writer io.Writer
}

// NewWriteStream always receives a *net.TcpConn as `writer`, except in
// testing it can accept a buffer as the fake writer.
func NewWriteStream(writer io.Writer) *WriteStream {
	return &WriteStream{
		writer: writer,
	}
}

// invoke an asynchronous write for message.
func (s *WriteStream) Write(msgBytes []byte) error {
	var err error
	var total = 0
	var written = 0

	toWrite := len(msgBytes)

	for total < toWrite && err == nil {
		written, err = s.writer.Write(msgBytes[total:])
		total += written
	}

	return err
}
