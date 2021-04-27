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

// TableHealthInfo is a report of replica health within the table.
type TableHealthInfo struct {
	PartitionCount int32
	Unhealthy      int32
	WriteUnhealthy int32
	ReadUnhealthy  int32
	FullHealthy    int32
}

// GetTableHealthInfo return *TableHealthInfo from meta.
func GetTableHealthInfo(meta Meta, tableName string) (*TableHealthInfo, error) {
	resp, err := meta.QueryConfig(tableName)
	if err != nil {
		return nil, err
	}

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
	}, nil
}
