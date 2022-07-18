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

package base

import (
	"fmt"

	"github.com/apache/thrift/lib/go/thrift"
)

type Blob struct {
	Data []byte
}

func (b *Blob) Read(iprot thrift.TProtocol) error {
	data, err := iprot.ReadBinary()
	if err != nil {
		return err
	}
	b.Data = data
	return nil
}

func (b *Blob) Write(oprot thrift.TProtocol) error {
	return oprot.WriteBinary(b.Data)
}

func (b *Blob) String() string {
	if b == nil {
		return "<nil>"
	}
	return fmt.Sprintf("Blob(%+v)", *b)
}

func NewBlob() *Blob {
	return &Blob{}
}
