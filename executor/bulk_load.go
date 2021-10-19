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

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
	"github.com/pegasus-kv/admin-cli/tabular"
	"github.com/pegasus-kv/admin-cli/util"
)

func StartBulkLoad(client *Client, tableName string, clusterName string, providerType string, rootPath string) error {
	err := client.Meta.StartBulkLoad(tableName, clusterName, providerType, rootPath)
	if err != nil {
		return err
	}
	fmt.Printf("Table %s start bulk load succeed\n", tableName)
	return nil
}

func PauseBulkLoad(client *Client, tableName string) error {
	err := client.Meta.PauseBulkLoad(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("Table %s pause bulk load succeed\n", tableName)
	return nil
}

func RestartBulkLoad(client *Client, tableName string) error {
	err := client.Meta.RestartBulkLoad(tableName)
	if err != nil {
		return err
	}
	fmt.Printf("Table %s restart bulk load succeed\n", tableName)
	return nil
}

func CancelBulkLoad(client *Client, tableName string, forced bool) error {
	err := client.Meta.CancelBulkLoad(tableName, forced)
	if err != nil {
		return err
	}
	fmt.Printf("Table %s cancel bulk load succeed\n", tableName)
	return nil
}

func QueryBulkLoad(client *Client, tableName string, partitionIndex int, detailed bool) error {
	resp, err := client.Meta.QueryBulkLoad(tableName)
	if err != nil {
		return err
	}
	partitionCount := len(resp.GetPartitionsStatus())
	if partitionIndex < -1 || partitionIndex >= partitionCount {
		return fmt.Errorf("Table %s query bulk load failed [hint: invalid partition index %d]", tableName, partitionIndex)
	}
	allPartitions := (partitionIndex == -1)
	if allPartitions {
		tableStatus := resp.GetAppStatus()
		switch tableStatus {
		case admin.BulkLoadStatus_BLS_DOWNLOADING:
			PrintAllDownloading(client, resp, detailed)
		case admin.BulkLoadStatus_BLS_SUCCEED, admin.BulkLoadStatus_BLS_FAILED, admin.BulkLoadStatus_BLS_CANCELED:
			PrintAllCleanupFlag(client, resp, detailed)
		default:
			PrintAllOthers(client, resp, detailed)
		}
	} else {
		partitionStatus := resp.GetPartitionsStatus()[partitionIndex]
		switch partitionStatus {
		case admin.BulkLoadStatus_BLS_DOWNLOADING:
			PrintSingleDownloading(client, resp, partitionIndex, detailed)
		case admin.BulkLoadStatus_BLS_INGESTING:
			PrintSingleIngesting(client, resp, partitionIndex, detailed)
		case admin.BulkLoadStatus_BLS_SUCCEED, admin.BulkLoadStatus_BLS_FAILED, admin.BulkLoadStatus_BLS_CANCELED:
			PrintSingleCleanupFlag(client, resp, partitionIndex, detailed)
		case admin.BulkLoadStatus_BLS_PAUSING:
			PrintSinglePausing(client, resp, partitionIndex, detailed)
		default:
			PrintSingleSummary(client, resp.GetAppName(), int32(partitionIndex), resp.GetPartitionsStatus()[partitionIndex])
		}
	}
	return nil
}

func PrintAllDownloading(client *Client, resp *admin.QueryBulkLoadResponse, detailed bool) {
	// calculate download progress
	partitionCount := len(resp.GetPartitionsStatus())
	var totalProgress int32 = 0
	partitionProgress := make(map[int]int32)
	for i, stateMap := range resp.GetBulkLoadStates() {
		var progress int32 = 0
		for _, pState := range stateMap {
			progress += pState.GetDownloadProgress()
		}
		progress /= resp.GetMaxReplicaCount()
		partitionProgress[i] = progress
		totalProgress += progress
	}
	totalProgress /= int32(partitionCount)
	// print summary info
	if !detailed {
		var tList []interface{}
		type summaryStruct struct {
			TName    string `json:"TableName"`
			TStatus  string `json:"TableStatus"`
			Progress int32  `json:"TotalDownloadProgress"`
		}
		tList = append(tList, summaryStruct{
			TName:    resp.GetAppName(),
			TStatus:  resp.GetAppStatus().String(),
			Progress: totalProgress,
		})
		tabular.Print(client, tList)
		return
	}
	// print detailed info
	var aList []interface{}
	type allStruct struct {
		Pidx     int32  `json:"PartitionIndex"`
		Status   string `json:"PartitionStatus"`
		Progress int32  `json:"DownloadProgress"`
	}
	for i := 0; i < partitionCount; i++ {
		aList = append(aList, allStruct{
			Pidx:     int32(i),
			Status:   resp.GetPartitionsStatus()[i].String(),
			Progress: partitionProgress[i],
		})
	}
	tabular.New(client, aList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("Name(%s)", resp.GetAppName()),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
			fmt.Sprintf("%d", totalProgress),
		})
	}).Render()
}

func PrintAllOthers(client *Client, resp *admin.QueryBulkLoadResponse, detailed bool) {
	partitionCount := len(resp.GetPartitionsStatus())
	if !detailed {
		PrintAllSummary(client, resp.GetAppName(), resp.GetAppStatus())
		return
	}
	var aList []interface{}
	type allStruct struct {
		Pidx   int32  `json:"PartitionIndex"`
		Status string `json:"PartitionStatus"`
	}
	for i := 0; i < partitionCount; i++ {
		aList = append(aList, allStruct{
			Pidx:   int32(i),
			Status: resp.GetPartitionsStatus()[i].String(),
		})
	}
	tabular.New(client, aList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("TableName(%s)", resp.GetAppName()),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
		})
	}).Render()
}

func PrintAllCleanupFlag(client *Client, resp *admin.QueryBulkLoadResponse, detailed bool) {
	partitionCount := len(resp.GetPartitionsStatus())
	if !detailed {
		PrintAllSummary(client, resp.GetAppName(), resp.GetAppStatus())
		return
	}
	var aList []interface{}
	type allStruct struct {
		Pidx    int32  `json:"PartitionIndex"`
		Status  string `json:"PartitionStatus"`
		Cleanup bool   `json:"IsCleanup"`
	}
	tableCleanup := true
	for i := 0; i < partitionCount; i++ {
		partitionMap := resp.GetBulkLoadStates()[i]
		partitionCleanup := int32(len(partitionMap)) == resp.GetMaxReplicaCount()
		for _, state := range partitionMap {
			partitionCleanup = partitionCleanup && state.GetIsCleanedUp()
		}
		aList = append(aList, allStruct{
			Pidx:    int32(i),
			Status:  resp.GetPartitionsStatus()[i].String(),
			Cleanup: partitionCleanup,
		})
		tableCleanup = tableCleanup && partitionCleanup
	}
	tabular.New(client, aList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("TableName(%s)", resp.GetAppName()),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
			fmt.Sprintf("%v", tableCleanup),
		})
	}).Render()
}

func PrintAllSummary(client *Client, tableName string, tableStatus admin.BulkLoadStatus) {
	var tList []interface{}
	type summaryStruct struct {
		TName   string `json:"TableName"`
		TStatus string `json:"TableStatus"`
	}
	tList = append(tList, summaryStruct{
		TName:   tableName,
		TStatus: tableStatus.String(),
	})
	tabular.Print(client, tList)
}

func PrintSingleDownloading(client *Client, resp *admin.QueryBulkLoadResponse, partitionIndex int, detailed bool) {
	stateMap := resp.GetBulkLoadStates()[partitionIndex]
	var partitionProgress int32 = 0
	for _, state := range stateMap {
		partitionProgress += state.GetDownloadProgress()
	}
	partitionProgress /= resp.GetMaxReplicaCount()
	// print summary info
	if !detailed {
		var tList []interface{}
		type summaryStruct struct {
			TName    string `json:"TableName"`
			Pidx     int32  `json:"Pidx"`
			PStatus  string `json:"PartitionStatus"`
			Progress int32  `json:"DownloadProgress"`
		}
		tList = append(tList, summaryStruct{
			TName:    resp.GetAppName(),
			Pidx:     int32(partitionIndex),
			PStatus:  resp.GetPartitionsStatus()[partitionIndex].String(),
			Progress: partitionProgress,
		})
		tabular.Print(client, tList)
		return
	}
	// print detailed info
	var sList []interface{}
	type singleStruct struct {
		Node     string `json:"NodeAddress"`
		PStatus  string `json:"PartitionStatus"`
		Progress int32  `json:"DownloadProgress"`
	}
	for node, state := range stateMap {
		sList = append(sList, singleStruct{
			Node:     node.String(),
			PStatus:  resp.GetPartitionsStatus()[partitionIndex].String(),
			Progress: state.GetDownloadProgress(),
		})
	}
	util.SortStructsByField(sList, "Node")
	tabular.New(client, sList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("Table(%s) Partition[%d]", resp.GetAppName(), partitionIndex),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
			fmt.Sprintf("%d", partitionProgress),
		})
	}).Render()
}

func PrintSingleIngesting(client *Client, resp *admin.QueryBulkLoadResponse, partitionIndex int, detailed bool) {
	stateMap := resp.GetBulkLoadStates()[partitionIndex]
	if !detailed {
		PrintSingleSummary(client, resp.GetAppName(), int32(partitionIndex), resp.GetPartitionsStatus()[partitionIndex])
		return
	}
	var sList []interface{}
	type singleStruct struct {
		Node    string `json:"NodeAddress"`
		PStatus string `json:"PartitionStatus"`
		IStatus string `json:"IngestionStatus"`
	}
	for node, state := range stateMap {
		sList = append(sList, singleStruct{
			Node:    node.String(),
			PStatus: resp.GetPartitionsStatus()[partitionIndex].String(),
			IStatus: state.GetIngestStatus().String(),
		})
	}
	util.SortStructsByField(sList, "Node")
	tabular.New(client, sList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("Table(%s) Partition[%d]", resp.GetAppName(), partitionIndex),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
			"",
		})
	}).Render()
}

func PrintSingleCleanupFlag(client *Client, resp *admin.QueryBulkLoadResponse, partitionIndex int, detailed bool) {
	stateMap := resp.GetBulkLoadStates()[partitionIndex]
	if !detailed {
		PrintSingleSummary(client, resp.GetAppName(), int32(partitionIndex), resp.GetPartitionsStatus()[partitionIndex])
		return
	}
	var sList []interface{}
	type singleStruct struct {
		Node    string `json:"NodeAddress"`
		PStatus string `json:"PartitionStatus"`
		Cleanup bool   `json:"IsCleanup"`
	}
	isCleanup := true
	for node, state := range stateMap {
		sList = append(sList, singleStruct{
			Node:    node.String(),
			PStatus: resp.GetPartitionsStatus()[partitionIndex].String(),
			Cleanup: state.GetIsCleanedUp(),
		})
		isCleanup = isCleanup && state.GetIsCleanedUp()
	}
	util.SortStructsByField(sList, "Node")
	tabular.New(client, sList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("Table(%s) Partition[%d]", resp.GetAppName(), partitionIndex),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
			fmt.Sprintf("%v", isCleanup),
		})
	}).Render()
}

func PrintSinglePausing(client *Client, resp *admin.QueryBulkLoadResponse, partitionIndex int, detailed bool) {
	stateMap := resp.GetBulkLoadStates()[partitionIndex]
	if !detailed {
		PrintSingleSummary(client, resp.GetAppName(), int32(partitionIndex), resp.GetPartitionsStatus()[partitionIndex])
		return
	}
	var sList []interface{}
	type singleStruct struct {
		Node    string `json:"NodeAddress"`
		PStatus string `json:"PartitionStatus"`
		Paused  bool   `json:"IsPaused"`
	}
	for node, state := range stateMap {
		sList = append(sList, singleStruct{
			Node:    node.String(),
			PStatus: resp.GetPartitionsStatus()[partitionIndex].String(),
			Paused:  state.GetIsPaused(),
		})
	}
	util.SortStructsByField(sList, "Node")
	tabular.New(client, sList, func(tbWriter *tablewriter.Table) {
		tbWriter.SetFooter([]string{
			fmt.Sprintf("Table(%s) Partition[%d]", resp.GetAppName(), partitionIndex),
			fmt.Sprintf("Table(%s)", resp.GetAppStatus().String()),
			"",
		})
	}).Render()
}

func PrintSingleSummary(client *Client, tableName string, pidx int32, partitionStatus admin.BulkLoadStatus) {
	var tList []interface{}
	type summaryStruct struct {
		TName   string `json:"TableName"`
		Pidx    int32  `json:"Pidx"`
		PStatus string `json:"PartitionStatus"`
	}
	tList = append(tList, summaryStruct{
		TName:   tableName,
		Pidx:    pidx,
		PStatus: partitionStatus.String(),
	})
	tabular.Print(client, tList)
}
