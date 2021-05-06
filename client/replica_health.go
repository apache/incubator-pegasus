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
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
)

// TableHealthInfo is a report of replica health within the table.
type TableHealthInfo struct {
	PartitionCount int32
	Unhealthy      int32
	WriteUnhealthy int32
	ReadUnhealthy  int32
	FullHealthy    int32
}

type NodeState struct {
	IPPort string

	Status admin.NodeStatus

	PrimariesNum   int
	SecondariesNum int
	ReplicaCount   int
}

// ClusterReplicaInfo is a report of the replicas distributed in the cluster.
type ClusterReplicaInfo struct {
	Tables []*TableHealthInfo
	Nodes  []*NodeState
}

// GetTableHealthInfo return *TableHealthInfo from meta.
func GetTableHealthInfo(meta Meta, tableName string) (*TableHealthInfo, error) {
	tbHealth, err := aggregateReplicaInfo(meta, tableName, nil)
	if err != nil {
		return nil, err
	}
	return tbHealth, nil
}

func aggregateReplicaInfo(meta Meta, tableName string, nodes *map[string]*NodeState) (*TableHealthInfo, error) {
	resp, err := meta.QueryConfig(tableName)
	if err != nil {
		return nil, err
	}
	if nodes != nil {
		aggregateNodeState(resp, nodes)
	}
	return aggregateTableHealthInfo(resp), nil
}

func aggregateTableHealthInfo(resp *replication.QueryCfgResponse) *TableHealthInfo {
	var fullHealthy, unHealthy, writeUnHealthy, readUnHealthy int32
	for _, partition := range resp.Partitions {
		var replicaCnt int32
		if partition.Primary.GetRawAddress() == 0 {
			writeUnHealthy++
			readUnHealthy++
		} else {
			replicaCnt = int32(len(partition.Secondaries) + 1)
			if replicaCnt >= partition.MaxReplicaCount {
				fullHealthy++
			} else if replicaCnt < 2 {
				writeUnHealthy++
			}
		}
	}

	unHealthy = resp.PartitionCount - fullHealthy
	return &TableHealthInfo{
		PartitionCount: resp.PartitionCount,
		Unhealthy:      unHealthy,
		WriteUnhealthy: writeUnHealthy,
		ReadUnhealthy:  readUnHealthy,
		FullHealthy:    fullHealthy,
	}
}

func aggregateNodeState(resp *replication.QueryCfgResponse, nodesPtr *map[string]*NodeState) {
	nodes := *nodesPtr
	for _, p := range resp.Partitions {
		if p.Primary.GetRawAddress() != 0 {
			naddr := p.Primary.GetAddress()
			nodes[naddr].PrimariesNum++
			nodes[naddr].ReplicaCount++
		}
		for _, sec := range p.Secondaries {
			naddr := sec.GetAddress()
			nodes[naddr].SecondariesNum++
			nodes[naddr].ReplicaCount++
		}
	}
}

// GetClusterReplicaInfo returns replica info in both node and table perspectives.
// = From node perspective, it returns a list of node states, each contains the replica details,
// including those with no replicas (empty node).
// = From table perspective, it returns a list of table states, each contains the partition health information.
func GetClusterReplicaInfo(meta Meta) (*ClusterReplicaInfo, error) {
	c := &ClusterReplicaInfo{}

	metaTables, err := meta.ListAvailableApps()
	if err != nil {
		return nil, err
	}

	// initialize the nodes in the cluster
	metaNodes, err := meta.ListNodes()
	if err != nil {
		return nil, err
	}
	nodesMap := map[string]*NodeState{}
	for _, node := range metaNodes {
		ipPort := node.GetAddress().GetAddress()
		nodesMap[ipPort] = &NodeState{
			IPPort: ipPort,
			Status: node.Status,
		}
	}

	for _, tb := range metaTables {
		tbHealthInfo, err := aggregateReplicaInfo(meta, tb.AppName, &nodesMap)
		if err != nil {
			return nil, err
		}
		c.Tables = append(c.Tables, tbHealthInfo)
	}

	for _, node := range nodesMap {
		c.Nodes = append(c.Nodes, node)
	}

	return c, nil
}
