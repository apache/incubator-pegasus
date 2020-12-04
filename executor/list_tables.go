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
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/pegasus-kv/admin-cli/executor/util"
	"github.com/pegasus-kv/admin-cli/tabular"
)

// ListTables command.
func ListTables(client *Client, showDropped bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	var status admin.AppStatus
	if showDropped {
		status = admin.AppStatus_AS_DROPPED
	} else {
		status = admin.AppStatus_AS_AVAILABLE
	}

	resp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: status,
	})
	if err != nil {
		return err
	}

	type droppedTableStruct struct {
		AppID          int32  `json:"ID"`
		Name           string `json:"Name"`
		PartitionCount int32  `json:"PartitionCount"`
		DropTime       string `json:"DropTime"`
		ExpireTime     string `json:"ExpireTime"`
	}

	type availableTableStruct struct {
		AppID           int32  `json:"ID"`
		Name            string `json:"Name"`
		PartitionCount  int32  `json:"Partitions"`
		UnHealthy       int32  `json:"Unhealthy"`
		WriteUnHealthy  int32  `json:"WriteUnhealthy"`
		ReadUnHealthy   int32  `json:"ReadUnhealthy"`
		CreateTime      string `json:"CreateTime"`
		WReqRateLimit   string `json:"WReqRateLimit"`
		WBytesRateLimit string `json:"WBytesRateLimit"`
	}

	var tbList []interface{}
	for _, tb := range resp.Infos {
		if status == admin.AppStatus_AS_AVAILABLE {
			unHealthy, writeUnHealthy, readUnHealthy, err := getPartitionHealthyCount(client, tb)
			if err != nil {
				return err
			}
			tbList = append(tbList, availableTableStruct{
				AppID:           tb.AppID,
				Name:            tb.AppName,
				UnHealthy:       unHealthy,
				WriteUnHealthy:  writeUnHealthy,
				ReadUnHealthy:   readUnHealthy,
				PartitionCount:  tb.PartitionCount,
				CreateTime:      util.FormatDate(tb.CreateSecond),
				WReqRateLimit:   tb.Envs["replica.write_throttling"],
				WBytesRateLimit: tb.Envs["replica.write_throttling_by_size"],
			})
		} else if status == admin.AppStatus_AS_DROPPED {
			tbList = append(tbList, droppedTableStruct{
				AppID:          tb.AppID,
				Name:           tb.AppName,
				DropTime:       util.FormatDate(tb.DropSecond),
				ExpireTime:     util.FormatDate(tb.ExpireSecond),
				PartitionCount: tb.PartitionCount,
			})
		}

	}

	// formats into tabular
	tabular.Print(client, tbList)
	return nil
}

// return (UnHealthy, WriteUnHealthy, ReadUnHealthy, Err)
func getPartitionHealthyCount(client *Client, table *admin.AppInfo) (int32, int32, int32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.Meta.QueryConfig(ctx, table.AppName)
	if err != nil {
		return 0, 0, 0, err
	}

	var fullHealthy, unHealthy, writeUnHealthy, readUnHealthy int32
	for _, partition := range resp.Partitions {
		var replicaCnt int32
		if partition.Primary == nil {
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
	unHealthy = table.PartitionCount - fullHealthy
	return unHealthy, writeUnHealthy, readUnHealthy, nil
}
