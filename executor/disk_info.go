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

package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/executor/util"
	"github.com/pegasus-kv/admin-cli/tabular"
)

type DiskInfoType int32

const (
	CapacitySize DiskInfoType = 0
	ReplicaCount DiskInfoType = 1
)

// QueryDiskInfo command
// TODO(jiashuo1) need refactor
func QueryDiskInfo(client *Client, infoType DiskInfoType, replicaServer string, tableName string, diskTag string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	n, err := client.Nodes.GetNode(replicaServer, session.NodeTypeReplica)
	if err != nil {
		return err
	}
	replica := n.Replica()

	resp, err := replica.QueryDiskInfo(ctx, &radmin.QueryDiskInfoRequest{
		Node:    &base.RPCAddress{}, //TODO(jiashuo1) this thrift variable is useless, it need be deleted on client/server
		AppName: tableName,
	})
	if err != nil {
		return err
	}

	switch infoType {
	case CapacitySize:
		queryDiskCapacity(client, n.TCPAddr(), resp, diskTag)
	case ReplicaCount:
		queryDiskReplicaCount(client, resp)
	default:
		break
	}
	return nil
}

func queryDiskCapacity(client *Client, replicaServer string, resp *radmin.QueryDiskInfoResponse, diskTag string) {

	type nodeCapacityStruct struct {
		Disk      string `json:"disk"`
		Capacity  int64  `json:"capacity"`
		Available int64  `json:"available"`
		Ratio     int64  `json:"ratio"`
	}

	type replicaCapacityStruct struct {
		Replica  string  `json:"replica"`
		Status   string  `json:"status"`
		Capacity float64 `json:"capacity"`
	}

	var nodeCapacityInfos []interface{}
	var replicaCapacityInfos []interface{}

	perfSession := client.Nodes.GetPerfSession(replicaServer, session.NodeTypeReplica)

	for _, diskInfo := range resp.DiskInfos {
		// pass disk tag means query one disk detail capacity of replica
		if len(diskTag) != 0 && diskInfo.Tag == diskTag {
			appendCapacity := func(replicasWithAppId map[int32][]*base.Gpid, replicaStatus string) {
				for _, replicas := range replicasWithAppId {
					for _, replica := range replicas {
						var gpidStr = fmt.Sprintf("%d.%d", replica.Appid, replica.PartitionIndex)
						replicaCapacityInfos = append(replicaCapacityInfos, replicaCapacityStruct{
							Replica:  gpidStr,
							Status:   replicaStatus,
							Capacity: float64(util.GetPartitionStat(perfSession, "disk.storage.sst(MB)", gpidStr)),
						})
					}
				}
			}
			appendCapacity(diskInfo.HoldingPrimaryReplicas, "primary")
			appendCapacity(diskInfo.HoldingSecondaryReplicas, "secondary")

			// formats into tabularWriter
			tabular.Print(client.Writer, replicaCapacityInfos)
			return
		}

		nodeCapacityInfos = append(nodeCapacityInfos, nodeCapacityStruct{
			Disk:      diskInfo.Tag,
			Capacity:  diskInfo.DiskCapacityMb,
			Available: diskInfo.DiskAvailableMb,
			Ratio:     diskInfo.DiskAvailableMb * 100.0 / diskInfo.DiskCapacityMb,
		})
	}

	tabular.Print(client.Writer, nodeCapacityInfos)
}

func queryDiskReplicaCount(client *Client, resp *radmin.QueryDiskInfoResponse) {
	type ReplicaCountStruct struct {
		Disk      string `json:"disk"`
		Primary   int    `json:"primary"`
		Secondary int    `json:"secondary"`
		Total     int    `json:"total"`
	}

	computeCount := func(replicasWithAppId map[int32][]*base.Gpid) int {
		var replicaCount = 0
		for _, replicas := range replicasWithAppId {
			for range replicas {
				replicaCount++
			}
		}
		return replicaCount
	}

	var replicaCountInfos []interface{}
	for _, diskInfo := range resp.DiskInfos {
		var primaryCount = computeCount(diskInfo.HoldingPrimaryReplicas)
		var secondaryCount = computeCount(diskInfo.HoldingSecondaryReplicas)
		replicaCountInfos = append(replicaCountInfos, ReplicaCountStruct{
			Disk:      diskInfo.Tag,
			Primary:   primaryCount,
			Secondary: secondaryCount,
			Total:     primaryCount + secondaryCount,
		})
	}

	tabular.Print(client.Writer, replicaCountInfos)
}
