package aggregate

import (
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// TableStatsAggregator aggregates the metric on each partition into table-level metrics.
// It's reponsible for all tables in the pegasus cluster.
// After all TableStats have been collected, TableStatsAggregator sums them up into a
// ClusterStats. Users of this pacakage can use the hooks to watch every changes of the stats.
type TableStatsAggregator interface {
	Aggregate() (map[int32]*TableStats, *ClusterStats)
}

// NewTableStatsAggregator returns a TableStatsAggregator instance.
func NewTableStatsAggregator(metaAddrs []string) TableStatsAggregator {
	return &tableStatsAggregator{
		tables: make(map[int32]*TableStats),
		client: newClient(metaAddrs),
	}
}

type tableStatsAggregator struct {
	tables   map[int32]*TableStats
	allStats *ClusterStats

	client *pegasusClient
}

// Start looping for metrics aggregation
func Start(tom *tomb.Tomb) {
	aggregateInterval := viper.GetDuration("metrics.report_interval")
	ticker := time.NewTicker(aggregateInterval)

	metaAddr := viper.GetString("meta_server")
	ag := NewTableStatsAggregator([]string{metaAddr})

	for {
		select {
		case <-tom.Dying(): // check if context cancelled
			return
		case <-ticker.C:
		}

		ag.Aggregate()
	}
}

func (ag *tableStatsAggregator) Aggregate() (map[int32]*TableStats, *ClusterStats) {
	ag.updateTableMap()

	// TODO(wutao1): reduce meta queries for listing nodes
	ag.client.updateNodes()
	for _, n := range ag.client.nodes {
		perfCounters, err := n.GetPerfCounters("@")
		if err != nil {
			log.Errorf("unable to query perf-counters: %s", err)
			return nil, nil
		}
		for _, p := range perfCounters {
			perfCounter := decodePartitionPerfCounter(p)
			if perfCounter == nil {
				continue
			}
			if !aggregatable(perfCounter) {
				continue
			}
			ag.updatePartitionStat(perfCounter)
		}
	}
	ag.extendStatsForAllPartitions()

	var batchTableStats []TableStats
	for _, table := range ag.tables {
		table.aggregate()
		batchTableStats = append(batchTableStats, *table)
	}
	ag.aggregateClusterStats()
	hooksManager.afterTableStatsEmitted(batchTableStats, *ag.allStats)

	return ag.tables, ag.allStats
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
func (ag *tableStatsAggregator) updateTableMap() {
	tables := ag.client.listTables()
	ag.doUpdateTableMap(tables)
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
func (ag *tableStatsAggregator) updatePartitionStat(pc *partitionPerfCounter) {
	tb, found := ag.tables[pc.gpid.Appid]
	if !found {
		// Ignore the perf-counter because there's currently no such table
		return
	}
	part, found := tb.Partitions[int(pc.gpid.PartitionIndex)]
	if !found {
		log.Errorf("no such partition %+v, perf-counter \"%s\"", pc.gpid, pc.name)
		return
	}
	part.update(pc)
}

func (ag *tableStatsAggregator) extendStatsForAllPartitions() {
	for _, tb := range ag.tables {
		for _, p := range tb.Partitions {
			extendStats(&p.Stats)
		}
	}
}
