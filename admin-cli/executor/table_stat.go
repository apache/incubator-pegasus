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
	"io"

	"github.com/apache/incubator-pegasus/admin-cli/tabular"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/collector/aggregate"
)

var tableStatsTemplate = `--- 
Request: 
 Get:
  counter: get_qps
 Mget:
  counter: multi_get_qps
 Scan:
  counter: scan_qps
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
 Abnormal:
  counter: recent_abnormal_count
 Expire:
  counter: recent_expire_count
 Filter:
  counter: recent_filter_count
Usage:
 KeyNum:
  counter: rdb_estimate_num_keys
 TableSize:
  counter: sst_storage_mb
  unit: MB
 AvgPartitionSize:
  counter: avg_partition_mb
  unit: MB
  aggregate: average
 Index:
  counter: rdb_index_and_filter_blocks_mem_usage
  unit: byte
 MemTable:
  counter: rdb_memtable_mem_usage
  unit: byte
`

// TableStat is table-stat command.
func TableStat(c *Client) error {
	ag := aggregate.NewTableStatsAggregator(c.Nodes.MetaAddresses)
	tableStats, _, err := ag.Aggregate()
	if err != nil {
		return err
	}
	// TODO(wutao): limit table count, if table count exceeds a number, the result
	// can be written to a file or somewhere instead.

	for _, tb := range tableStats {
		// Calculate average partition size. This counter could help to diagnose
		// if there is table that has abnormally large partitions.
		tb.Stats["avg_partition_mb"] = tb.Stats["sst_storage_mb"] / float64(len(tb.Partitions))
	}

	printTableStatsTabular(c, tableStats)
	return nil
}

// printTableStatsTabular prints table stats in a number of sections,
// according to the predefined template.
func printTableStatsTabular(writer io.Writer, tables map[int32]*aggregate.TableStats) {
	t := tabular.NewTemplate(tableStatsTemplate)
	t.SetCommonColumns([]string{"AppID", "Name", "Partitions"}, func(rowData interface{}) []string {
		tbStat := rowData.(aggregate.TableStats)
		return []string{fmt.Sprint(tbStat.AppID), tbStat.TableName, fmt.Sprint(len(tbStat.Partitions))}
	})
	t.SetColumnValueFunc(func(col *tabular.ColumnAttributes, rowData interface{}) interface{} {
		tbStat := rowData.(aggregate.TableStats)
		return tbStat.Stats[col.Attrs["counter"]]
	})

	var valueList []interface{}
	for _, tb := range tables {
		valueList = append(valueList, *tb)
	}
	util.SortStructsByField(valueList, "AppID")
	t.Render(writer, valueList)
}
