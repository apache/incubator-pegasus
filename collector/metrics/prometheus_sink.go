package metrics

import (
	"sync"

	"github.com/pegasus-kv/collector/aggregate"
	"github.com/prometheus/client_golang/prometheus"
)

type prometheusMetricFamily struct {
	metrics map[string]prometheus.Gauge
}

func (f *prometheusMetricFamily) set(name string, value float64) {
	f.metrics[name].Set(value)
}

type prometheusSink struct {
	tableMap map[int]*prometheusMetricFamily

	tableLock sync.RWMutex

	clusterMetric *prometheusMetricFamily

	allTrackedMetrics []string
}

func newPrometheusSink() *prometheusSink {
	sink := &prometheusSink{
		tableMap:          make(map[int]*prometheusMetricFamily),
		allTrackedMetrics: aggregate.AllMetrics(),
	}
	sink.clusterMetric = sink.newClusterMetricFamily()

	aggregate.AddHookAfterTableDropped(func(appID int) {
		// remove the metrics family belongs to the table
		sink.tableLock.Lock()
		for _, gauge := range sink.tableMap[appID].metrics {
			prometheus.Unregister(gauge)
		}
		delete(sink.tableMap, appID)
		sink.tableLock.Unlock()
	})
	return sink
}

func (sink *prometheusSink) Report(stats []aggregate.TableStats, allStats aggregate.ClusterStats) {
	for _, table := range stats {
		sink.tableLock.Lock()
		defer sink.tableLock.Unlock()

		var mfamily *prometheusMetricFamily
		var found bool
		if mfamily, found = sink.tableMap[table.AppID]; !found {
			mfamily = sink.newTableMetricFamily(table.TableName)
			// insert table metrics family
			sink.tableMap[table.AppID] = mfamily
		}
		fillStatsIntoGauges(table.Stats, mfamily)
	}
	fillStatsIntoGauges(allStats.Stats, sink.clusterMetric)
}

func fillStatsIntoGauges(stats map[string]float64, family *prometheusMetricFamily) {
	for name, value := range stats {
		family.set(name, value)
	}
}

func (sink *prometheusSink) newTableMetricFamily(tableName string) *prometheusMetricFamily {
	return sink.newMetricFamily(map[string]string{"table": tableName, "entity": "table"})
}

func (sink *prometheusSink) newClusterMetricFamily() *prometheusMetricFamily {
	return sink.newMetricFamily(map[string]string{"entity": "cluster"})
}

func (sink *prometheusSink) newMetricFamily(labels map[string]string) *prometheusMetricFamily {
	mfamily := &prometheusMetricFamily{
		metrics: make(map[string]prometheus.Gauge),
	}
	for _, m := range sink.allTrackedMetrics {
		// create and register a gauge
		opts := prometheus.GaugeOpts{
			Name:        m,
			ConstLabels: labels,
		}
		gauge := prometheus.NewGauge(opts)
		prometheus.MustRegister(gauge)
		mfamily.metrics[m] = gauge
	}
	return mfamily
}
