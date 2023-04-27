// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package aggregate

import (
	"testing"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/stretchr/testify/assert"
)

func TestDecodePartitionPerfCounter(t *testing.T) {
	tests := []struct {
		name string

		isNil          bool
		counterName    string
		appID          int32
		partitionIndex int32
	}{
		{name: "replica*app.pegasus*get_latency@2.5.p999", isNil: true},

		// server-level counter, does not contain gpid.
		{name: "replica*eon.replica*table.level.RPC_RRDB_RRDB_CHECK_AND_MUTATE.latency(ns)@temp", isNil: true},
		{
			name:  "replica*eon.replica*table.level.RPC_RRDB_RRDB_MULTI_PUT.latency(ns)@temp.p999",
			isNil: true,
		},

		{
			name:           "replica*app.pegasus*recent.abnormal.count@1.2",
			counterName:    "replica*app.pegasus*recent.abnormal.count",
			appID:          1,
			partitionIndex: 2,
		},
	}

	for _, tt := range tests {
		pc := decodePartitionPerfCounter(tt.name, 1.0)
		assert.Equal(t, pc == nil, tt.isNil, tt.name)
		if pc != nil {
			assert.Equal(t, pc.name, tt.counterName)
			assert.Equal(t, pc.gpid, base.Gpid{Appid: tt.appID, PartitionIndex: tt.partitionIndex})
		}
	}
}
