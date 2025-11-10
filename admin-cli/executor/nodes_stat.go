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
	"github.com/apache/incubator-pegasus/admin-cli/tabular"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/collector/aggregate"
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
  aggregate: average
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
  unit: byte
 MGetBytes:
  counter: replica*app.pegasus*multi_get_bytes
  unit: byte
 PutBytes:
  counter: replica*app.pegasus*put_bytes
  unit: byte
 MputBytes:
  counter: replica*app.pegasus*multi_put_bytes
  unit: byte
`

func ShowNodesStat(client *Client) error {
	nodesStats, err := util.GetNodeStats(client.Perf)
	if err != nil {
		return err
	}
	printNodesStatsTabular(client, nodesStats)
	return nil
}

func printNodesStatsTabular(client *Client, nodes map[string]*aggregate.NodeStat) {
	t := tabular.NewTemplate(nodeStatsTemplate)
	t.SetCommonColumns([]string{"Node"}, func(rowData interface{}) []string {
		node := rowData.(aggregate.NodeStat)
		return []string{client.Nodes.MustGetReplica(node.Addr).CombinedAddr()}
	})
	t.SetColumnValueFunc(func(col *tabular.ColumnAttributes, rowData interface{}) interface{} {
		node := rowData.(aggregate.NodeStat)
		return node.Stats[col.Attrs["counter"]]
	})

	var valueList []interface{}
	for _, n := range nodes {
		valueList = append(valueList, *n)
	}
	util.SortStructsByField(valueList, "Addr")
	t.Render(client, valueList)
}
