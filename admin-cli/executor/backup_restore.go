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
	"time"

	"github.com/apache/incubator-pegasus/admin-cli/tabular"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
)

func BackupTable(client *Client, tableID int, providerType string, backupPath string) error {
	resp, errRecall := client.Meta.StartBackupApp(tableID, providerType, backupPath)
	if errRecall != nil {
		return errRecall
	}
	fmt.Printf("[hint]: %s\n", resp.GetHintMessage())
	fmt.Printf("BackupID: %d\n", resp.GetBackupID())
	return nil
}

func QueryBackupStatus(client *Client, tableID int, backupID int64) error {
	resp, errRecall := client.Meta.QueryBackupStatus(tableID, backupID)
	if errRecall != nil {
		return errRecall
	}
	fmt.Printf("[hint]: %s\n", resp.GetHintMessage())

	type backupItemStruct struct {
		BackupID           int64  `json:"BackupID"`
		AppName            string `json:"AppName"`
		StartTime          string `json:"StartTime"`
		EndTime            string `json:"EndTime"`
		Status             string `json:"Status"`
		BackupProviderType string `json:"BackupProviderType"`
		BackupPath         string `json:"BackupPath"`
	}

	var tbList []interface{}
	for _, item := range resp.GetBackupItems() {
		var status string
		if item.IsBackupFailed {
			status = "failed"
		} else if item.EndTimeMs == 0 {
			status = "in progress"
		} else {
			status = "completed"
		}

		startTime := time.Unix(item.StartTimeMs/1000, 0).String()
		var endTime string
		if item.EndTimeMs == 0 {
			endTime = "-"
		} else {
			endTime = time.Unix(item.EndTimeMs/1000, 0).String()
		}

		tbList = append(tbList, backupItemStruct{
			BackupID:           item.BackupID,
			AppName:            item.AppName,
			StartTime:          startTime,
			EndTime:            endTime,
			Status:             status,
			BackupProviderType: item.BackupProviderType,
			BackupPath:         item.BackupPath,
		})
	}
	// formats into tabular
	tabular.Print(client, tbList)
	return nil
}

func RestoreTable(client *Client, oldClusterName string, oldTableName string, oldTableID int,
	backupID int64, providerType string, newTableName string, restorePath string, skipBadPartition bool, policyName string) error {
	resp, errRecall := client.Meta.RestoreApp(
		oldClusterName, oldTableName, oldTableID, backupID, providerType, newTableName, restorePath, skipBadPartition, policyName)
	if errRecall != nil {
		return errRecall
	}

	fmt.Fprintf(client, "new AppID: %d\n", resp.GetAppid())
	errWait := waitTableBecomeHealthy(client, newTableName)

	if errWait != nil {
		return errWait
	}

	return nil
}

func waitTableBecomeHealthy(c *Client, tableName string) error {
	fmt.Fprintf(c, "Wait table \"%s\" become healthy ...\n", tableName)
	resp, err := c.Meta.QueryConfig(tableName)
	if err != nil {
		return err
	}
	if resp.GetErr().Errno != base.ERR_OK.String() {
		return fmt.Errorf("QueryConfig failed: %s", resp.GetErr().String())
	}

	partitionCount := resp.PartitionCount
	var replicaCount int32
	replicaCount = 0
	for _, part := range resp.Partitions {
		maxReplicaCount := part.GetMaxReplicaCount()
		if maxReplicaCount > replicaCount {
			replicaCount = maxReplicaCount
		}
	}
	return waitTableReady(c, tableName, int(partitionCount), int(replicaCount))
}
