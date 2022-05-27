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
	"fmt"
	"testing"

	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/rand"
)

type fakeNode struct {
	primaries   map[base.Gpid]bool
	secondaries map[base.Gpid]bool

	n *util.PegasusNode
}

type fakeCluster struct {
	nodes []*fakeNode

	meta *fakeMeta
}

func newFakeCluster(num int) *fakeCluster {
	c := &fakeCluster{
		nodes: make([]*fakeNode, num),
		meta:  &fakeMeta{},
	}
	for i := range c.nodes { // 127.0.0.1:34800, 127.0.0.1:34801, 127.0.0.1:34802 and so on
		ipPort := fmt.Sprintf("127.0.0.1:3480%d", i)
		c.nodes[i] = &fakeNode{
			n:           util.NewNodeFromTCPAddr(ipPort, session.NodeTypeReplica),
			primaries:   map[base.Gpid]bool{},
			secondaries: map[base.Gpid]bool{},
		}
	}
	return c
}

func rand3Nodes() []*fakeNode {
	nodeNum := len(fakePegasusCluster.nodes)

	chosenSet := map[int]bool{}
	for len(chosenSet) < 3 { // loop until we find 3 distinct nodes
		chosenSet[rand.Intn(nodeNum)] = true
	}

	var ret []*fakeNode
	for nidx := range chosenSet {
		ret = append(ret, fakePegasusCluster.nodes[nidx])
	}
	return ret
}

func createFakeTable(tbName string, partitionNum int) {
	appID, _ := fakePegasusCluster.meta.CreateApp(tbName, map[string]string{}, partitionNum)

	// evenly assign replicas to nodes
	for i := 0; i < partitionNum; i++ {
		nodes := rand3Nodes()

		nodes[0].primaries[base.Gpid{Appid: appID, PartitionIndex: int32(i)}] = true
		nodes[1].secondaries[base.Gpid{Appid: appID, PartitionIndex: int32(i)}] = true
		nodes[2].secondaries[base.Gpid{Appid: appID, PartitionIndex: int32(i)}] = true
	}
}

var fakePegasusCluster *fakeCluster

func TestFakeClusterRand3Nodes(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)
	assert.Equal(t, len(fakePegasusCluster.nodes), 4)

	nodes := rand3Nodes()
	assert.Equal(t, len(nodes), 3)
}

// ensure we are testing upon a correct foundation
func TestFakeCluster(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)

	testCases := []struct {
		tbName       string
		partitionNum int
	}{
		{tbName: "test1", partitionNum: 16},
		{tbName: "test2", partitionNum: 32},
		{tbName: "test3", partitionNum: 64},
		{tbName: "test4", partitionNum: 128},
	}

	for _, tt := range testCases {
		createFakeTable(tt.tbName, tt.partitionNum)
	}

	assertReplicasNotOnSameNode(t)

	expectedTotalPartitions := 16 + 32 + 64 + 128
	assertNoMissingReplicaInCluster(t, expectedTotalPartitions)
}

func TestFakeQueryConfigNeverReturnNilPrimary(t *testing.T) {
	fakePegasusCluster = newFakeCluster(4)
	createFakeTable("test", 128)
	meta := fakePegasusCluster.meta

	resp, _ := meta.QueryConfig("test")
	part := resp.Partitions[0]
	_ = shutdownReplica(meta, part.GetPid(), part.Primary)

	resp, _ = meta.QueryConfig("test")
	part = resp.Partitions[0]
	assert.NotNil(t, part.Primary) // never nil, since this field is non-optional

	assert.Equal(t, part.MaxReplicaCount, int32(3))
}

// ensure 3-replica for every partitions are not distributed on the same node.
func assertReplicasNotOnSameNode(t *testing.T) {
	tables, _ := fakePegasusCluster.meta.ListAvailableApps()
	for _, tb := range tables {
		resp, _ := fakePegasusCluster.meta.QueryConfig(tb.AppName)
		assert.Equal(t, len(resp.Partitions), int(tb.PartitionCount))

		for _, p := range resp.Partitions {
			for _, sec := range p.Secondaries {
				assert.NotEqual(t, p.Primary.GetAddress(), sec.GetAddress())
			}
			if len(p.Secondaries) >= 2 {
				assert.NotEqual(t, p.Secondaries[0].GetAddress(), p.Secondaries[1].GetAddress())
			}
		}
	}
}

func assertNoMissingReplicaInCluster(t *testing.T, expectedTotalPartitions int) {
	actualPri := 0
	actualSec := 0
	for _, n := range fakePegasusCluster.nodes {
		actualPri += len(n.primaries)
		actualSec += len(n.secondaries)
	}
	assert.Equal(t, actualPri, expectedTotalPartitions)
	assert.Equal(t, actualSec, expectedTotalPartitions*2)
}
