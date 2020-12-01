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

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/cheggaaa/pb/v3"
)

// CreateTable command
func CreateTable(c *Client, tableName string, partitionCount int) error {
	if partitionCount < 1 {
		return fmt.Errorf("partitions count should >=1")
	}
	// TODO(wutao): reject request with invalid table name

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.CreateApp(ctx, &admin.CreateAppRequest{
		AppName: tableName,
		Options: &admin.CreateAppOptions{
			PartitionCount: int32(partitionCount),
			IsStateful:     true,
			AppType:        "pegasus",
			SuccessIfExist: true,
			ReplicaCount:   3,
		},
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(c, "Creating table \"%s\" (AppID: %d)\n", tableName, resp.Appid)
	return waitTableReady(c, tableName, partitionCount)
}

func waitTableReady(c *Client, tableName string, partitionCount int) error {
	fmt.Fprintf(c, "Available partitions:\n")
	bar := pb.Full.Start(partitionCount) // Add a new bar

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		resp, err := c.Meta.QueryConfig(ctx, tableName)
		if err != nil {
			return err
		}
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return fmt.Errorf("QueryConfig failed: %s", resp.GetErr().String())
		}

		readyCount := 0
		for _, part := range resp.Partitions {
			if part.Primary.GetRawAddress() != 0 && len(part.Secondaries)+1 == 3 {
				readyCount++
			}
		}
		bar.SetCurrent(int64(readyCount))
		if readyCount == partitionCount {
			break
		}
		time.Sleep(time.Second)
	}

	bar.Finish()
	fmt.Fprintf(c, "Done!\n")
	return nil
}
