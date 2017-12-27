// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package rpc

import (
	"io"
)

type RpcReadStream struct {
	reader io.Reader
}

func (r *RpcReadStream) Next(size int) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := r.reader.Read(buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func NewRpcReadStream(reader io.Reader) *RpcReadStream {
	return &RpcReadStream{
		reader: reader,
	}
}
