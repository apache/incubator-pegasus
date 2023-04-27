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
	"fmt"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// TableStatsAggregator aggregates the metric on each partition into table-level metrics.
// It's reponsible for all tables in the pegasus cluster.
// After all TableStats have been collected, TableStatsAggregator sums them up into a
// ClusterStats. Users of this pacakage can use the hooks to watch every changes of the stats.
type TableStatsAggregator interface {
	Aggregate() (map[int32]*TableStats, *ClusterStats, error)

	Close()
}

// NewTableStatsAggregator returns a TableStatsAggregator instance.
func NewTableStatsAggregator(metaAddrs []string) TableStatsAggregator {
	return &tableStatsAggregator{
		tables: make(map[int32]*TableStats),
		client: NewPerfClient(metaAddrs),
	}
}

type tableStatsAggregator struct {
	tables   map[int32]*TableStats
	allStats *ClusterStats

	client *PerfClient
}

// Start looping for metrics aggregation
func Start(tom *tomb.Tomb) {
	aggregateInterval := viper.GetDuration("metrics.report_interval")
	ticker := time.NewTicker(aggregateInterval)

	metaAddr := viper.GetString("meta_server")
	iAg := NewTableStatsAggregator([]string{metaAddr})
	ag := iAg.(*tableStatsAggregator)

	for {
		select {
		case <-tom.Dying(): // check if context cancelled
			return
		case <-ticker.C:
		}

		_, _, err := ag.Aggregate()
		if err != nil {
			log.Error(err)
		}

		// produce stats for the hooks
		var batchTableStats []TableStats
		for _, table := range ag.tables {
			batchTableStats = append(batchTableStats, *table)
		}
		ag.aggregateClusterStats()
		hooksManager.afterTableStatsEmitted(batchTableStats, *ag.allStats)
	}
}

func (ag *tableStatsAggregator) Aggregate() (map[int32]*TableStats, *ClusterStats, error) {
	err := ag.updateTableMap()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to aggregate: %s", err)
	}

	// TODO(wutao1): reduce meta queries for listing nodes
	partitions, err := ag.client.GetPartitionStats()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to aggregate: %s", err)
	}
	for _, p := range partitions {
		ag.updatePartitionStat(p)
	}

	for _, table := range ag.tables {
		table.aggregate()
	}

	return ag.tables, ag.allStats, nil
}

func (ag *tableStatsAggregator) Close() {
	ag.client.Close()
}

func (ag *tableStatsAggregator) aggregateClusterStats() {
	ag.allStats = &ClusterStats{
		Stats:     make(map[string]float64),
		Timestamp: time.Now(),
	}
	for _, table := range ag.tables {
		for k, v := range table.Stats {
			ag.allStats.Stats[k] += v
		}
	}
}

// Some tables may disappear (be dropped) or first show up.
// This function maintains the local table map
// to keep consistent with the pegasus cluster.
func (ag *tableStatsAggregator) updateTableMap() error {
	tables, err := ag.client.listTables()
	if err != nil {
		return err
	}
	ag.doUpdateTableMap(tables)
	return nil
}

func (ag *tableStatsAggregator) doUpdateTableMap(tables []*admin.AppInfo) {
	currentTableSet := make(map[int32]*struct{})
	for _, tb := range tables {
		currentTableSet[tb.AppID] = nil
		if _, found := ag.tables[tb.AppID]; !found {
			// non-exisistent table, create it
			ag.tables[tb.AppID] = newTableStats(tb)
			log.Infof("found new table: %+v", tb)

			// TODO(wutao1): some tables may have partitions splitted,
			//               recreate the tableStats then.
		}
	}
	for appID, tb := range ag.tables {
		// disappeared table, delete it
		if _, found := currentTableSet[appID]; !found {
			log.Infof("remove table from collector: {AppID: %d, PartitionCount: %d}", appID, len(tb.Partitions))
			delete(ag.tables, appID)

			hooksManager.afterTableDropped(appID)
		}
	}
}

// Update the counter value.
func (ag *tableStatsAggregator) updatePartitionStat(pc *PartitionStats) {
	tb, found := ag.tables[pc.Gpid.Appid]
	if !found {
		// Ignore the perf-counter because there's currently no such table
		return
	}
	part, found := tb.Partitions[int(pc.Gpid.PartitionIndex)]
	if !found {
		log.Errorf("no such partition %+v", pc.Gpid)
		return
	}
	*part = *pc
}
