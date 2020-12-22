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
	"github.com/klauspost/compress/zstd"
)

// BytesCompression is an interface of algorithm for compressing bytes.
type BytesCompression interface {
	Compress([]byte) ([]byte, error)

	Decompress([]byte) ([]byte, error)
}

type zstdCompression struct {
	encoder *zstd.Encoder
	decoder *zstd.Decoder
}

func (z *zstdCompression) Compress(data []byte) ([]byte, error) {
	return z.encoder.EncodeAll(data, make([]byte, 0, len(data))), nil
}

func (z *zstdCompression) Decompress(data []byte) ([]byte, error) {
	return z.decoder.DecodeAll(data, nil)
}

// TODO(wutao): compression command is not implemented yet.
//
// func newZstdCompression() BytesCompression {
// 	decoder, _ := zstd.NewReader(nil)
// 	encoder, _ := zstd.NewWriter(nil)

// 	return &zstdCompression{
// 		encoder: encoder,
// 		decoder: decoder,
// 	}
// }
