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
	"sort"
	"testing"

	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/stretchr/testify/assert"
)

func newReplicaFromRPCAddr(addr *base.RPCAddress) *util.PegasusNode {
	return util.NewNodeFromTCPAddr(addr.GetAddress(), session.NodeTypeReplica)
}

func shutdownReplica(meta Meta, gpid *base.Gpid, node *base.RPCAddress) error {
	return meta.Propose(gpid, admin.ConfigType_CT_DOWNGRADE_TO_INACTIVE, nil, newReplicaFromRPCAddr(node))
}

func TestGetTableHealthInfo(t *testing.T) {
	testCases := []struct {
		injectErr func(meta Meta, part *replication.PartitionConfiguration)

		wUnhealthy int32
		rUnhealthy int32
		unhealthy  int32
	}{
		{
			func(meta Meta, part *replication.PartitionConfiguration) {
				_ = shutdownReplica(meta, part.GetPid(), part.Secondaries[0])
			},
			// no partition is unhealthy, because all of them still have 2 replicas.
			0, 0, 1,
		},
		{
			func(meta Meta, part *replication.PartitionConfiguration) {
				_ = shutdownReplica(meta, part.GetPid(), part.Secondaries[0])
				_ = shutdownReplica(meta, part.GetPid(), part.Secondaries[1])
			},
			1, 0, 1,
		},
		{
			func(meta Meta, part *replication.PartitionConfiguration) {
				_ = shutdownReplica(meta, part.GetPid(), part.Primary)
			},
			1, 1, 1,
		},
	}

	for _, tt := range testCases {
		fakePegasusCluster = newFakeCluster(4)
		createFakeTable("test", 128)
		meta := fakePegasusCluster.meta

		resp, _ := meta.QueryConfig("test")
		tt.injectErr(meta, resp.Partitions[0])

		tb, err := GetTableHealthInfo(meta, "test")
		assert.NoError(t, err)

		assert.Equal(t, tb.WriteUnhealthy, tt.wUnhealthy)
		assert.Equal(t, tb.ReadUnhealthy, tt.rUnhealthy)
		assert.Equal(t, tb.Unhealthy, tt.unhealthy)
	}
}

func TestGetClusterReplicaInfo(t *testing.T) {
	fakePegasusCluster = newFakeCluster(9)
	createFakeTable("test1", 32)
	createFakeTable("test2", 64)
	createFakeTable("test3", 128)
	meta := fakePegasusCluster.meta

	{
		// all nodes must have primaries/secondaries
		c, _ := GetClusterReplicaInfo(meta)
		for i := 0; i < len(c.Nodes); i++ {
			assert.NotZero(t, c.Nodes[i].PrimariesNum)
			assert.NotZero(t, c.Nodes[i].SecondariesNum)
			assert.NotZero(t, c.Nodes[i].ReplicaCount)
		}
	}

	{
		effectedReplicas := map[base.Gpid]int{}
		safelyDowngradeNode(t, fakePegasusCluster.nodes[0], &effectedReplicas)

		c, _ := GetClusterReplicaInfo(meta)
		assert.Equal(t, len(c.Nodes), 9)
		assert.Equal(t, len(c.Tables), 3)

		// sort nodes by ipport
		sort.Slice(c.Nodes, func(i, j int) bool {
			return c.Nodes[i].IPPort < c.Nodes[j].IPPort
		})
		assert.Equal(t, c.Nodes[0].PrimariesNum, 0)
		assert.Equal(t, c.Nodes[0].SecondariesNum, 0)
		assert.Equal(t, c.Nodes[0].ReplicaCount, 0)
		for i := 1; i < len(c.Nodes); i++ {
			assert.NotZero(t, c.Nodes[i].PrimariesNum)
			assert.NotZero(t, c.Nodes[i].SecondariesNum)
			assert.NotZero(t, c.Nodes[i].ReplicaCount)
		}
	}
}
