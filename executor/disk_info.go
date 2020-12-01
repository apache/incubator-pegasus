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
	"admin-cli/helper"
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/olekukonko/tablewriter"
)

type DiskInfoType int32

const (
	CapacitySize DiskInfoType = 0
	ReplicaCount DiskInfoType = 1
)

// QueryDiskInfo command
func QueryDiskInfo(client *Client, infoType DiskInfoType, replicaServer string, tableName string, diskTag string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var addr, err = helper.Resolve(replicaServer, helper.Host2Addr)
	if err == nil {
		replicaServer = addr
	}

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
		_ = queryDiskCapacity(client, replicaServer, resp, diskTag)
	case ReplicaCount:
		queryDiskReplicaCount(client, resp)
	default:
		break
	}
	return nil
}

func queryDiskCapacity(client *Client, replicaServer string, resp *radmin.QueryDiskInfoResponse, diskTag string) error {

	type nodeCapacityStruct struct {
		Disk      string
		Capacity  int64
		Available int64
		Ratio     int64
	}

	type replicaCapacityStruct struct {
		Replica  string
		Status   string
		Capacity float64
	}

	var nodeCapacityInfos []nodeCapacityStruct
	var replicaCapacityInfos []replicaCapacityStruct

	perfClient, err := client.GetPerfCounterClient(replicaServer)
	if err != nil {
		return err
	}

	for _, diskInfo := range resp.DiskInfos {
		// pass disk tag means query one disk detail capacity of replica
		if len(diskTag) != 0 && diskInfo.Tag == diskTag {
			appendReplicaCapacityInfo := func(replicasWithAppId map[int32][]*base.Gpid, replicaStatus string) {
				for _, replicas := range replicasWithAppId {
					for _, replica := range replicas {
						var gpidStr = fmt.Sprintf("%d.%d", replica.Appid, replica.PartitionIndex)
						replicaCapacityInfos = append(replicaCapacityInfos, replicaCapacityStruct{
							Replica:  gpidStr,
							Status:   replicaStatus,
							Capacity: float64(helper.GetReplicaCounterValue(perfClient, "disk.storage.sst(MB)", gpidStr)),
						})
					}
				}
			}
			appendReplicaCapacityInfo(diskInfo.HoldingPrimaryReplicas, "primary")
			appendReplicaCapacityInfo(diskInfo.HoldingSecondaryReplicas, "secondary")

			// formats into tabular
			tabular := tablewriter.NewWriter(client)
			tabular.SetHeader([]string{"Replica", "Status", "Capacity"})
			tabular.SetAutoFormatHeaders(false)
			tabular.SetAlignment(tablewriter.ALIGN_CENTER)
			for _, replicaCapacityInfo := range replicaCapacityInfos {
				tabular.Append([]string{
					replicaCapacityInfo.Replica,
					replicaCapacityInfo.Status,
					strconv.FormatFloat(replicaCapacityInfo.Capacity, 'f', -1, 64)})
			}
			tabular.Render()
			return nil
		}

		nodeCapacityInfos = append(nodeCapacityInfos, nodeCapacityStruct{
			Disk:      diskInfo.Tag,
			Capacity:  diskInfo.DiskCapacityMb,
			Available: diskInfo.DiskAvailableMb,
			Ratio:     diskInfo.DiskAvailableMb * 100.0 / diskInfo.DiskCapacityMb,
		})
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	tabular.SetHeader([]string{"Disk", "Capacity", "Available", "Ratio"})
	for _, nodeCapacityInfo := range nodeCapacityInfos {
		tabular.Append([]string{
			nodeCapacityInfo.Disk,
			strconv.FormatInt(nodeCapacityInfo.Capacity, 10),
			strconv.FormatInt(nodeCapacityInfo.Available, 10),
			strconv.FormatInt(nodeCapacityInfo.Ratio, 10)})
	}
	tabular.Render()
	return nil
}

func queryDiskReplicaCount(client *Client, resp *radmin.QueryDiskInfoResponse) {
	type ReplicaCountStruct struct {
		Disk      string
		Primary   int
		Secondary int
		Total     int
	}

	computeReplicaCount := func(replicasWithAppId map[int32][]*base.Gpid) int {
		var replicaCount = 0
		for _, replicas := range replicasWithAppId {
			for range replicas {
				replicaCount++
			}
		}
		return replicaCount
	}

	var replicaCountInfos []ReplicaCountStruct
	for _, diskInfo := range resp.DiskInfos {
		var primaryCount = computeReplicaCount(diskInfo.HoldingPrimaryReplicas)
		var secondaryCount = computeReplicaCount(diskInfo.HoldingSecondaryReplicas)
		replicaCountInfos = append(replicaCountInfos, ReplicaCountStruct{
			Disk:      diskInfo.Tag,
			Primary:   primaryCount,
			Secondary: secondaryCount,
			Total:     primaryCount + secondaryCount,
		})
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetHeader([]string{"Disk", "Primary", "Secondary", "Total"})
	tabular.SetAlignment(tablewriter.ALIGN_CENTER)
	for _, replicaCountInfo := range replicaCountInfos {
		tabular.Append([]string{
			replicaCountInfo.Disk,
			strconv.Itoa(replicaCountInfo.Primary),
			strconv.Itoa(replicaCountInfo.Secondary),
			strconv.Itoa(replicaCountInfo.Total)})
	}
	tabular.Render()
}
