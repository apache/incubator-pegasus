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
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/base"
)

// PartitionStats is a set of metrics retrieved from this partition.
type PartitionStats struct {
	Gpid base.Gpid

	// Address of the primary where this partition locates.
	Addr string

	// perfCounter's name -> the value.
	Stats map[string]float64
}

// TableStats has the aggregated metrics for this table.
type TableStats struct {
	TableName string
	AppID     int

	Partitions map[int]*PartitionStats

	// the time when the stats was generated
	Timestamp time.Time

	// The aggregated value of table metrics.
	// perfCounter's name -> the value.
	Stats map[string]float64
}

// ClusterStats is the aggregated metrics for all the TableStats in this cluster.
// For example, 3 tables with "write_qps" [25, 70, 100] are summed up to
// `Stats: {"write_qps" : 195}`.
type ClusterStats struct {
	Timestamp time.Time

	Stats map[string]float64
}

func newTableStats(info *admin.AppInfo) *TableStats {
	tb := &TableStats{
		TableName:  info.AppName,
		AppID:      int(info.AppID),
		Partitions: make(map[int]*PartitionStats),
		Stats:      make(map[string]float64),
		Timestamp:  time.Now(),
	}
	for i := 0; i < int(info.PartitionCount); i++ {
		tb.Partitions[i] = &PartitionStats{
			Gpid:  base.Gpid{Appid: int32(info.AppID), PartitionIndex: int32(i)},
			Stats: make(map[string]float64),
		}
	}
	return tb
}

func (tb *TableStats) aggregate() {
	tb.Timestamp = time.Now()
	for _, part := range tb.Partitions {
		for name, value := range part.Stats {
			tb.Stats[name] += value
		}
	}
}

func aggregateCustomStats(elements []string, stats *map[string]float64, resultName string) {
	aggregated := float64(0)
	for _, ele := range elements {
		if v, found := (*stats)[ele]; found {
			aggregated += v
		}
	}
	(*stats)[resultName] = aggregated
}

// Extends the stat with read_qps/read_bytes/write_qps/write_bytes.
func extendStats(stats *map[string]float64) {
	var reads = []string{
		"get",
		"multi_get",
		"scan",
	}
	var readQPS []string
	for _, r := range reads {
		readQPS = append(readQPS, r+"_qps")
	}
	var readBytes []string
	for _, r := range reads {
		readBytes = append(readBytes, r+"_bytes")
	}
	aggregateCustomStats(readQPS, stats, "read_qps")
	aggregateCustomStats(readBytes, stats, "read_bytes")

	var writes = []string{
		"put",
		"remove",
		"multi_put",
		"multi_remove",
		"check_and_set",
		"check_and_mutate",
	}
	var writeQPS []string
	for _, w := range writes {
		writeQPS = append(writeQPS, w+"_qps")
	}
	var writeBytes []string
	for _, w := range writes {
		writeBytes = append(writeBytes, w+"_bytes")
	}
	aggregateCustomStats(writeQPS, stats, "write_qps")
	aggregateCustomStats(writeBytes, stats, "write_bytes")
}
