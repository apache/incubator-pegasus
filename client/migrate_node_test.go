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
package client

import (
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/stretchr/testify/assert"
)

func TestMigratePrimariesOut(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)
	createFakeTable("test", 16)
	assertReplicasNotOnSameNode(t)

	for i := range fakePegasusCluster.nodes {
		replicaServer := fakePegasusCluster.nodes[i]
		err := MigratePrimariesOut(fakePegasusCluster.meta, replicaServer.n)
		assert.NoError(t, err)

		assertReplicasNotOnSameNode(t)

		assert.Empty(t, replicaServer.primaries)
		assertNoMissingReplicaInCluster(t, 16)
	}
}

func TestDowngradeNode(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)
	createFakeTable("test", 16)

	// Downgrade 2 nodes, at this moment the primaries will still be safe,
	// but the secondaries will be effected. Some may be effected twice.
	for i := 0; i < 2; i++ {
		effectedReplicas := map[base.Gpid]int{}
		replicaServer := fakePegasusCluster.nodes[1]
		effectedReplicas = safelyDowngradeNode(t, replicaServer, effectedReplicas)

		resp, _ := fakePegasusCluster.meta.QueryConfig("test")
		for _, p := range resp.Partitions {
			if times, ok := effectedReplicas[*p.Pid]; ok {
				assert.Equal(t, len(p.Secondaries), 2-times)
			}
			assert.NotEqual(t, p.Primary.GetRawAddress(), 0)
		}
	}
}

// safelyDowngradeNode returns the effected partitions.
// NOTE: map[base.Gpid]int, `int` is the times that this partition has been downgraded until now.
func safelyDowngradeNode(t *testing.T, replicaServer *fakeNode, effectedReplicas map[base.Gpid]int) map[base.Gpid]int {
	// ensure no primary on this node
	_ = MigratePrimariesOut(fakePegasusCluster.meta, replicaServer.n)

	for r := range replicaServer.primaries {
		effectedReplicas[r]++
	}
	for r := range replicaServer.secondaries {
		effectedReplicas[r]++
	}
	err := DowngradeNode(fakePegasusCluster.meta, replicaServer.n)
	assert.NoError(t, err)
	return effectedReplicas
}

// ensure when the node has primaries running, downgrade will fail.
func TestDowngradeNodeHasPrimaries(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)
	createFakeTable("test", 16)

	// node must have no primary
	err := DowngradeNode(fakePegasusCluster.meta, fakePegasusCluster.nodes[0].n)
	assert.Error(t, err)
}
