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

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/cheggaaa/pb/v3"
)

// CreateTable command
func CreateTable(c *Client, tableName string, partitionCount int, replicaCount int) error {
	if partitionCount < 1 {
		return fmt.Errorf("partitions count should >=1")
	}
	if replicaCount < 1 {
		return fmt.Errorf("replica count should >=1")
	}
	// TODO(wutao): reject request with invalid table name

	appID, err := c.Meta.CreateApp(tableName, map[string]string{}, partitionCount)
	if err != nil {
		return err
	}

	fmt.Fprintf(c, "Creating table \"%s\" (AppID: %d)\n", tableName, appID)
	return waitTableReady(c, tableName, partitionCount, replicaCount)
}

func waitTableReady(c *Client, tableName string, partitionCount int, replicaCount int) error {
	fmt.Fprintf(c, "Available partitions:\n")
	bar := pb.Full.Start(partitionCount) // Add a new bar

	for {
		resp, err := c.Meta.QueryConfig(tableName)
		if err != nil {
			return err
		}
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return fmt.Errorf("QueryConfig failed: %s", resp.GetErr().String())
		}

		readyCount := 0
		for _, part := range resp.Partitions {
			if part.Primary.GetRawAddress() != 0 && len(part.Secondaries)+1 == replicaCount {
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
