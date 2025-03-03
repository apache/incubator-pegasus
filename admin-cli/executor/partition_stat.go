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
	"sort"

	"github.com/apache/incubator-pegasus/admin-cli/tabular"
	"github.com/apache/incubator-pegasus/collector/aggregate"
)

var partitionStatsTemplate = `---
Overall:
 Get:
  counter: get_qps
 Mget:
  counter: multi_get_qps
 RBytes:
  counter: read_bytes
  unit: byte
 Put:
  counter: put_qps
 Mput:
  counter: multi_put_qps
 CAM:
  counter: check_and_mutate_qps
 CAS:
  counter: check_and_set_qps
 Incr:
  counter: incr_qps
 WBytes:
  counter: write_bytes
  unit: byte
 Size:
  counter: sst_storage_mb
  unit: MB
 Abnormal:
  counter: recent_abnormal_count
`

// ShowPartitionsStats is partition-stat command
func ShowPartitionsStats(client *Client, tableName string) error {
	resp, err := client.Meta.QueryConfig(tableName)
	if err != nil {
		return err
	}
	appID := resp.AppID

	// filter out the partitions belongs to this table.
	var appPartitions []*aggregate.PartitionStats
	partitions, err := client.Perf.GetPartitionStats()
	if err != nil {
		return err
	}
	for _, p := range partitions {
		if p.Gpid.Appid == appID {
			appPartitions = append(appPartitions, p)
		}
	}
	// sort by partition index
	sort.Slice(appPartitions, func(i, j int) bool {
		return appPartitions[i].Gpid.PartitionIndex < appPartitions[j].Gpid.PartitionIndex
	})

	t := tabular.NewTemplate(partitionStatsTemplate)
	t.SetCommonColumns([]string{"Pidx"}, func(rowData interface{}) []string {
		partition := rowData.(*aggregate.PartitionStats)
		return []string{fmt.Sprint(partition.Gpid.PartitionIndex)}
	})
	t.SetColumnValueFunc(func(col *tabular.ColumnAttributes, rowData interface{}) interface{} {
		partition := rowData.(*aggregate.PartitionStats)
		return partition.Stats[col.Attrs["counter"]]
	})

	var valueList []interface{}
	for _, n := range appPartitions {
		valueList = append(valueList, n)
	}
	t.Render(client, valueList)

	return nil
}
