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

	"github.com/ghodss/yaml"
	"github.com/pegasus-kv/admin-cli/tabular"
	"github.com/pegasus-kv/collector/aggregate"
)

var tableStatsTemplate = `--- 
Memory: 
 IndexMem:
  counter: rdb_index_and_filter_blocks_mem_usage
  unit: byte
 MemTbMem:
  counter: rdb_memtable_mem_usage
  unit: byte
Peformance: 
 Abnormal:
  counter: recent_abnormal_count
 Expire:
  counter: recent_expire_count
 Filter:
  counter: recent_filter_count
Read: 
 Get:
  counter: get_qps
 Mget:
  counter: multi_get_qps
 Scan:
  counter: scan_qps
 RBytes:
  counter: read_bytes
  unit: byte
 WBytes:
  counter: write_bytes
  unit: byte
Storage: 
 KeyNum:
  counter: rdb_estimate_num_keys
 SSTStorege:
  counter: sst_storage_mb
  unit: MB
Write: 
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
`

// TableStat is table-stat command.
func TableStat(c *Client) error {
	ag := aggregate.NewTableStatsAggregator(c.Nodes.MetaAddresses)
	tableStats, _ := ag.Aggregate()
	// TODO(wutao): limit table count, if table count exceeds a number, the result
	// can be written to a file or somewhere instead.

	printTableStatsTabular(c, tableStats)
	return nil
}

// PrintTableStatsTabular prints table stats in a number of sections,
// according to the predefined template.
func printTableStatsTabular(writer io.Writer, tables map[int32]*aggregate.TableStats) {
	var sections map[string]interface{}
	err := yaml.Unmarshal([]byte(tableStatsTemplate), &sections)
	if err != nil {
		panic(err)
	}

	for sect, columns := range sections {
		// print section
		fmt.Printf("[%s]\n", sect)

		header := []string{"AppID", "Name", "Partitions"}
		var counters []map[string]interface{}
		var formatters []tabular.StatFormatter
		for key, attrs := range columns.(map[string]interface{}) {
			attrsMap := attrs.(map[string]interface{})

			header = append(header, key)
			counters = append(counters, attrsMap)
			formatters = tabular.FormatStat(attrsMap, formatters)
		}

		tabWriter := tabular.NewTabWriter(writer)
		tabWriter.SetHeader(header)
		for _, tb := range tables {
			// each table displays as a row
			var row []string
			row = append(row, fmt.Sprintf("%d", tb.AppID), tb.TableName, fmt.Sprintf("%d", len(tb.Partitions)))
			for i, kv := range counters {
				row = append(row, formatters[i](tb.Stats[kv["counter"].(string)]))
			}
			tabWriter.Append(row)
		}
		tabWriter.Render()
	}
}
