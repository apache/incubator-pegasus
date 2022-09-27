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
	"testing"

	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

func TestGpid(t *testing.T) {
	pegalog.SetLogger(pegalog.StderrLogger)
	type testCase struct {
		gpidObject *Gpid
		gpidBytes  []byte
	}

	testCases := []testCase{
		{&Gpid{
			Appid:          0,
			PartitionIndex: 0}, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0},
		},
		{&Gpid{
			Appid:          0,
			PartitionIndex: 2}, []byte{0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0},
		},
		{&Gpid{
			Appid:          1,
			PartitionIndex: 0}, []byte{0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1},
		},
		{&Gpid{
			Appid:          1,
			PartitionIndex: 2}, []byte{0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x1},
		},
	}

	for _, testCase := range testCases {
		// test gpid serialize
		buf := thrift.NewTMemoryBuffer()
		oprot := thrift.NewTBinaryProtocolTransport(buf)
		testCase.gpidObject.Write(oprot)
		oprot.WriteMessageEnd()
		assert.Equal(t, buf.Bytes(), testCase.gpidBytes)

		// test gpid deserialize
		iprot := thrift.NewTBinaryProtocolTransport(buf)
		readGpid := &Gpid{}
		readGpid.Read(iprot)
		iprot.ReadMessageEnd()
		assert.Equal(t, readGpid, testCase.gpidObject)
	}
}
