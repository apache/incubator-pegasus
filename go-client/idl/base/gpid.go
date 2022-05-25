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

type Gpid struct {
	Appid, PartitionIndex int32
}

func (id *Gpid) Read(iprot thrift.TProtocol) error {
	v, err := iprot.ReadI64()
	if err != nil {
		return err
	}

	id.Appid = int32(v & int64(0x00000000ffffffff))
	id.PartitionIndex = int32(v >> 32)
	return nil
}

func (id *Gpid) Write(oprot thrift.TProtocol) error {
	v := int64(id.Appid) + int64(id.PartitionIndex)<<32
	return oprot.WriteI64(v)
}

func (id *Gpid) String() string {
	if id == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%+v", *id)
}
