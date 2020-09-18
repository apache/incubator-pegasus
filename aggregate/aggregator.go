package aggregate

import (
	"time"

	"github.com/pegasus-kv/collector/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// TableStatsAggregator aggregates the metric on each partition into table-level metrics.
// It's reponsible for all tables in the pegasus cluster.
type TableStatsAggregator interface {

	// Start reporting until the ctx cancelled. This method will block the current thread.
	Start(tomb *tomb.Tomb)
}

// NewTableStatsAggregator returns a TableStatsAggregator instance.
func NewTableStatsAggregator() TableStatsAggregator {
	metaAddr := viper.GetString("meta_server")
	return &tableStatsAggregator{
		aggregateInterval: viper.GetDuration("metrics.report_interval"),
		metaClient:        client.NewMetaClient(metaAddr),
		tables:            make(map[int]*TableStats),
	}
}

type tableStatsAggregator struct {
	aggregateInterval time.Duration

	metaClient client.MetaClient
	tables     map[int]*TableStats
}

func (ag *tableStatsAggregator) Start(tom *tomb.Tomb) {
	ticker := time.NewTicker(ag.aggregateInterval)
	for {
		select {
		case <-tom.Dying(): // check if context cancelled
			return
		case <-ticker.C:
		}

		ag.aggregate()
	}
}

func (ag *tableStatsAggregator) aggregate() {
	ag.updateTableMap()

	nodes, err := ag.metaClient.ListNodes()
	if err != nil {
		log.Error(err)
		return
	}
	for _, n := range nodes {
		rcmdClient := client.NewRemoteCmdClient(n.Addr)
		perfCounters, err := rcmdClient.GetPerfCounters("@")
		if err != nil {
			log.Errorf("unable to query perf-counters: %s", err)
			return
		}
		for _, p := range perfCounters {
			perfCounter, err := decodePartitionPerfCounter(p)
			if err != nil {
				log.Error(err)
				continue
			}
			ag.updatePartitionStat(perfCounter)
		}
	}
	for _, table := range ag.tables {
		table.aggregate()

		hooksManager.afterTablStatsEmitted(*table)
	}
}

// Some tables may disappear (be dropped) or first show up.
// This function maintains the local table map
// to keep consistent with the pegasus cluster.
func (ag *tableStatsAggregator) updateTableMap() {
	tables, err := ag.metaClient.ListTables()
	if err != nil {
		log.Error(err)
		return
	}
	ag.doUpdateTableMap(tables)
}

func (ag *tableStatsAggregator) doUpdateTableMap(tables []*client.TableInfo) {
	currentTableSet := make(map[int]*struct{})
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

func (ag *tableStatsAggregator) updatePartitionStat(pc *partitionPerfCounter) {
	tb, found := ag.tables[int(pc.gpid.Appid)]
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
