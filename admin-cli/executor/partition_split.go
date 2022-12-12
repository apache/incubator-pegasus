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
	"fmt"

	"github.com/apache/incubator-pegasus/admin-cli/tabular"
)

func StartPartitionSplit(client *Client, tableName string, newPartitionCount int) error {
	if err := DiskBeforeSplit(client, tableName); err != nil {
		return err
	}
	if err := client.Meta.StartPartitionSplit(tableName, newPartitionCount); err != nil {
		return err
	}
	fmt.Printf("Table %s start partition split succeed\n", tableName)
	return nil
}

func QuerySplitStatus(client *Client, tableName string) error {
	resp, err := client.Meta.QuerySplitStatus(tableName)
	if err != nil {
		return err
	}
	// show all partitions split status
	type splitStatusStruct struct {
		PartitionIndex int32  `json:"Pidx"`
		Status         string `json:"SplitStatus"`
	}
	fmt.Println("[AllPartitionStatus]")
	var pList []interface{}
	var i int32 = 0
	statusMap := resp.GetStatus()
	for ; i < resp.GetNewPartitionCount_()/2; i++ {
		var status string
		value, ok := statusMap[i]
		if ok {
			status = value.String()
		} else {
			status = "COMPLETED"
		}
		pList = append(pList, splitStatusStruct{
			PartitionIndex: i,
			Status:         status,
		})
	}
	tabular.Print(client, pList)

	// show summary count
	fmt.Println("[Summary]")
	var sList []interface{}
	type summaryStruct struct {
		SplittingCount int32 `json:"SplittingCount"`
		CompletedCount int32 `json:"CompletedCount"`
	}
	sList = append(sList, summaryStruct{
		SplittingCount: int32(len(statusMap)),
		CompletedCount: resp.GetNewPartitionCount_()/2 - int32(len(statusMap)),
	})
	tabular.Print(client, sList)
	return nil
}

func PausePartitionSplit(client *Client, tableName string, parentPidx int) error {
	err := client.Meta.PausePartitionSplit(tableName, parentPidx)
	if err != nil {
		return err
	}
	if parentPidx < 0 {
		fmt.Printf("Table %s all partition pause split succeed\n", tableName)
	} else {
		fmt.Printf("Table %s partition[%d] pause split succeed\n", tableName, parentPidx)
	}
	return nil
}

func RestartPartitionSplit(client *Client, tableName string, parentPidx int) error {
	err := client.Meta.RestartPartitionSplit(tableName, parentPidx)
	if err != nil {
		return err
	}
	if parentPidx < 0 {
		fmt.Printf("Table %s all partition restart split succeed\n", tableName)
	} else {
		fmt.Printf("Table %s partition[%d] restart split succeed\n", tableName, parentPidx)
	}
	return nil
}

func CancelPartitionSplit(client *Client, tableName string, oldPartitionCount int) error {
	err := client.Meta.CancelPartitionSplit(tableName, oldPartitionCount)
	if err != nil {
		return err
	}
	fmt.Printf("Table %s cancel partition split succeed\n", tableName)
	return nil
}

type NodeDiskStats struct {
	NodeAddress     string
	DiskTag         string
	DiskCapacity    int64
	DiskAvailable   int64
	ReplicaCapacity []ReplicaCapacityStruct
}

func DiskBeforeSplit(client *Client, tableName string) error {
	// get queryDiskInfoResponse for all replica nodes
	respMap, err := QueryAllNodesDiskInfo(client, tableName)
	if err != nil {
		return fmt.Errorf("%s [hint: failed to query disk info when disk check before split]", err)
	}
	// get NodeDiskStats for the table partition on each disk
	var nList []NodeDiskStats
	for address, resp := range respMap {
		for _, diskInfo := range resp.GetDiskInfos() {
			diskTag := diskInfo.GetTag()
			rCapacityList, err := ConvertReplicaCapacityStruct(fillDiskCapacity(client, address, resp, diskTag, false))
			if err != nil {
				return fmt.Errorf("%s [hint: failed to get info for node(%s) disk(%s) when disk check before split]", err, address, diskTag)
			}
			nList = append(nList, NodeDiskStats{
				NodeAddress:     address,
				DiskTag:         diskTag,
				DiskCapacity:    diskInfo.GetDiskCapacityMb(),
				DiskAvailable:   diskInfo.GetDiskAvailableMb(),
				ReplicaCapacity: rCapacityList,
			})
		}
	}
	// check disk space before split
	for _, nodeDiskStats := range nList {
		var totalSize int64 = 0
		for _, rCapacity := range nodeDiskStats.ReplicaCapacity {
			totalSize += rCapacity.Size
		}
		diskUsedAfterSplit := totalSize*3 + nodeDiskStats.DiskCapacity - nodeDiskStats.DiskAvailable
		diskThreshold := nodeDiskStats.DiskCapacity * 9 / 10
		if diskUsedAfterSplit > diskThreshold {
			return fmt.Errorf("disk(%s@%s) doesn't have enough space to execute partition split[after(%v) vs capacity(%v)]", nodeDiskStats.NodeAddress, nodeDiskStats.DiskTag, diskUsedAfterSplit, diskThreshold)
		}
	}
	return nil
}
