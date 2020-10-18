package metrics

import (
	"github.com/pegasus-kv/collector/aggregate"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Sink is the destination where the metrics are reported to.
type Sink interface {

	// Report the snapshot of metrics to the monitoring system. The report can possibly fail.
	Report(stats []aggregate.TableStats, allStats aggregate.ClusterStats)
}

// NewSink creates a Sink which reports metrics to the configured monitoring system.
func NewSink() Sink {
	var sink Sink
	cfgSink := viper.Get("metrics.sink")
	if cfgSink == "falcon" {
		sink = newFalconSink()
	} else if cfgSink == "prometheus" {
		sink = newPrometheusSink()
	} else {
		log.Fatalf("invalid metrics_sink = %s", cfgSink)
		return nil
	}

	aggregate.AddHookAfterTableStatEmitted(func(stats []aggregate.TableStats, allStats aggregate.ClusterStats) {
		go func() {
			sink.Report(stats, allStats)
		}()
	})

	return sink
}
