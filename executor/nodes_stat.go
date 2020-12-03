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
	"github.com/pegasus-kv/admin-cli/tabular"

	"github.com/ghodss/yaml"
	"github.com/pegasus-kv/admin-cli/executor/util"
	"github.com/pegasus-kv/collector/aggregate"
)

var nodeStatsTemplate = `---
Usage:
 DiskTotal:
  counter: replica*eon.replica_stub*disk.capacity.total(MB)
  unit: MB
 DiskAvailable:
  counter: replica*eon.replica_stub*disk.available.total(MB)
  unit: MB
 DiskAvaRatio(%):
  counter: replica*eon.replica_stub*disk.available.total.ratio
 MemUsed:
  counter: replica*server*memused.res(MB)
  unit: MB
 BlockCache:
  counter: replica*app.pegasus*rdb.block_cache.memory_usage
  unit: byte
 IndexMem:
  counter: replica*app.pegasus*rdb.index_and_filter_blocks.memory_usage
  unit: byte
Request:
 Get:
  counter: replica*app.pegasus*get_qps
 Mget:
  counter: replica*app.pegasus*multi_get_qps
 Put:
  counter: replica*app.pegasus*put_qps
 Mput:
  counter: replica*app.pegasus*multi_put_qps
 GetBytes:
  counter: replica*app.pegasus*get_bytes
 MGetBytes:
  counter: replica*app.pegasus*multi_get_bytes
 PutBytes:
  counter: replica*app.pegasus*put_bytes
 MputBytes:
  counter: replica*app.pegasus*multi_put_bytes
`

func ShowNodesStat(client *Client, detail bool) error {
	nodesStats := util.GetNodeStat(client.Perf)
	printNodesStatsTabular(client, nodesStats, detail)
	return nil
}

func printNodesStatsTabular(client *Client, nodes map[string]*aggregate.NodeStat, detail bool) {
	var sections map[string]interface{}
	err := yaml.Unmarshal([]byte(nodeStatsTemplate), &sections)
	if err != nil {
		panic(err)
	}

	for sect, columns := range sections {
		// print section
		if !detail && sect != "Usage" {
			continue
		}
		fmt.Printf("[%s]\n", sect)

		header := []string{"Node"}
		var counters []map[string]interface{}
		var formatters []tabular.StatFormatter
		for key, attrs := range columns.(map[string]interface{}) {
			attrsMap := attrs.(map[string]interface{})

			header = append(header, key)
			counters = append(counters, attrsMap)
			formatters = tabular.FormatStat(attrsMap, formatters)
		}

		tabWriter := tabular.NewTabWriter(client.Writer)
		tabWriter.SetHeader(header)
		for _, node := range nodes {
			// each table displays as a row
			var row []string
			row = append(row, client.Nodes.MustGetReplica(node.Addr).CombinedAddr())
			for i, kv := range counters {
				row = append(row, formatters[i](node.Stats[kv["counter"].(string)]))
			}
			tabWriter.Append(row)
		}
		tabWriter.Render()
	}
}
