// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package aggregate

import (
	"testing"

	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/stretchr/testify/assert"
)

func TestUpdateLocalTableMap(t *testing.T) {
	ag := &tableStatsAggregator{
		client: NewPerfClient([]string{"127.0.0.1:34601"}),
		tables: make(map[int32]*TableStats),
	}
	err := ag.updateTableMap()
	assert.Nil(t, err)
	assert.Equal(t, len(ag.tables), 2)
	assert.Equal(t, len(ag.tables[1].Partitions), 4) // test
	assert.Equal(t, len(ag.tables[2].Partitions), 8) // stat

	tables := []*admin.AppInfo{
		{AppID: 1, AppName: "stat", PartitionCount: 4},
		{AppID: 2, AppName: "test", PartitionCount: 8},
		{AppID: 3, AppName: "new_table", PartitionCount: 16},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 3)
	assert.Equal(t, len(ag.tables[3].Partitions), 16)

	tables = []*admin.AppInfo{
		{AppID: 1, AppName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 1)
	assert.Equal(t, len(ag.tables[1].Partitions), 4)
}

func TestUpdatePartitionStats(t *testing.T) {
	ag := &tableStatsAggregator{
		tables: make(map[int32]*TableStats),
	}
	tables := []*admin.AppInfo{
		{AppID: 1, AppName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)

	pc := decodePartitionPerfCounter("replica*app.pegasus*recent.abnormal.count@1.2", 100)
	assert.NotNil(t, pc)

	ag.updatePartitionStat(&PartitionStats{
		Gpid: base.Gpid{Appid: 1, PartitionIndex: 2},
		Addr: "127.0.0.1:34601",
		Stats: map[string]float64{
			"replica*app.pegasus*recent.abnormal.count": 100,
		},
	})

	part := ag.tables[1].Partitions[2]
	assert.Contains(t, part.Stats, pc.name)
	assert.Equal(t, part.Stats[pc.name], float64(100))
	assert.Equal(t, part.Addr, "127.0.0.1:34601")
}

func TestAggregate(t *testing.T) {
	ag := NewTableStatsAggregator([]string{"127.0.0.1:34601"})
	tableStats, allStat, err := ag.Aggregate()
	assert.Nil(t, err)
	assert.Greater(t, len(allStat.Stats), 0)

	assert.Equal(t, len(tableStats), 2)

	// ensure len(tableStats) == len(partitionStats) == len(clusterStats)
	for _, tb := range tableStats {
		assert.Equal(t, len(tb.Stats), len(allStat.Stats))
		for _, p := range tb.Partitions {
			assert.Equal(t, len(p.Stats), len(allStat.Stats))
		}
	}
}
