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

func TestPerfClientGetNodeStats(t *testing.T) {
	pclient := NewPerfClient([]string{"127.0.0.1:34601"})
	nodes, err := pclient.GetNodeStats("@")
	assert.Nil(t, err)
	assert.Greater(t, len(nodes), 0)
	assert.Greater(t, len(nodes[0].Stats), 0)
	for _, n := range nodes {
		assert.NotEmpty(t, n.Addr)
	}
}

func TestPerfClientGetPartitionStats(t *testing.T) {
	pclient := NewPerfClient([]string{"127.0.0.1:34601"})
	partitions, err := pclient.GetPartitionStats()
	assert.Nil(t, err)
	assert.Greater(t, len(partitions), 0)
	assert.Greater(t, len(partitions[0].Stats), 0)
	for _, p := range partitions {
		assert.NotEmpty(t, p.Addr)
		assert.NotEqual(t, p.Gpid, base.Gpid{Appid: 0, PartitionIndex: 0})
		assert.NotEmpty(t, p.Stats)
	}
}
