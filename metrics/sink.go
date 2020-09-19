package metrics

import (
	"github.com/pegasus-kv/collector/aggregate"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type MetricSnapshot struct {
	name  string
	value float64
	tags  map[string]string
}

// Sink is the destination where the metrics are reported to.
type Sink interface {

	// Report the snapshot of metrics to the monitoring system. The report can possibly fail.
	Report(snapshots []*MetricSnapshot)
}

// NewSink creates a Sink which reports metrics to the configured monitoring system.
func NewSink() Sink {
	var sink Sink
	cfgSink := viper.Get("metrics.sink")
	if cfgSink == "falcon" {
		sink = newFalconSink()
	} else if cfgSink == "prometheus" {
		sink = &prometheusSink{}
	} else {
		log.Fatalf("invalid metrics_sink = %s", cfgSink)
		return nil
	}

	aggregate.AddHookAfterTableStatEmitted(func(stats []aggregate.TableStats) {
		// periodically take snapshot of all the metrics and push them to the sink.
		var snapshots []*MetricSnapshot
		for _, table := range stats {
			for name, value := range table.Stats {
				snapshots = append(snapshots, &MetricSnapshot{
					name:  name,
					value: value,
					tags: map[string]string{
						"table":  table.TableName,
						"entity": "table",
					},
				})
			}
		}
		go func() { // run asynchronously to avoid blocking in the hook
			sink.Report(snapshots)
		}()
	})

	return sink
}
