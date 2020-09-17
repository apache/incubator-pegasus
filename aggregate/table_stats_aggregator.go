package aggregate

import (
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/collector/client"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// TableStatsAggregator aggregates the metric on each partition into table-level metrics.
type TableStatsAggregator interface {

	// Start reporting until the ctx cancelled. This method will block the current thread.
	Start(tomb *tomb.Tomb)
}

// NewTableStatsAggregator returns a TableStatsAggregator instance.
func NewTableStatsAggregator() TableStatsAggregator {
	metaAddrs := viper.GetStringSlice("meta_servers")
	return &tableStatsAggregator{
		aggregateInterval: viper.GetDuration("metrics.report_interval"),
		metaClient:        client.NewMetaClient(metaAddrs),
	}
}

type tableStatsAggregator struct {
	aggregateInterval time.Duration

	metaClient client.MetaClient
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
	nodes, err := ag.metaClient.ListNodes()
	if err != nil {
		log.Error(err)
		return
	}

	for _, n := range nodes {
		rcmdClient := client.NewRemoteCmdClient(n.Addr)
		perfCounters, err := rcmdClient.GetPerfCounters(".*@.*")
		if err != nil {
			log.Errorf("unable to query perf-counters: %s", err)
			return
		}
		for _, p := range perfCounters {
			ag.decodePartitionStat(p)
		}
	}
}

func (ag *tableStatsAggregator) decodePartitionStat(counter *client.PerfCounter) {

}

type partitionStats struct {
	gpid base.Gpid

	// perfCounter's name -> the value.
	stats map[string]float64
}

type tableStats struct {
	appID      int
	partitions map[int]*partitionStats

	// perfCounter's name -> the value.
	stats map[string]float64
}
