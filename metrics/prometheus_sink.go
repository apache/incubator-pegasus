package metrics

import (
	"net/http"
	"sync"

	"github.com/pegasus-kv/collector/aggregate"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func init() {
	http.Handle("/metrics", promhttp.Handler())
}

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

func (sink *prometheusSink) dropTable(appID int) {
	// remove the metrics family belongs to the table
	sink.tableLock.Lock()
	delete(sink.tableMap, appID)
	sink.tableLock.Unlock()
}

func newPrometheusSink() *prometheusSink {
	sink := &prometheusSink{
		tableMap:          make(map[int]*prometheusMetricFamily),
		allTrackedMetrics: aggregate.AllMetrics(),
	}
	sink.clusterMetric = sink.newClusterMetricFamily()

	aggregate.AddHookAfterTableDropped(func(appID int) {
		sink.dropTable(appID)
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
	mfamily := &prometheusMetricFamily{}
	for _, m := range sink.allTrackedMetrics {
		opts := prometheus.GaugeOpts{
			Name:        m,
			ConstLabels: labels,
		}
		mfamily.metrics[m] = prometheus.NewGauge(opts)
	}
	return mfamily
}
