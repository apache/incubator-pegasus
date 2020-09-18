package metrics

import (
	"time"

	"github.com/pegasus-kv/collector/aggregate"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// Reporter reports metrics of this Collector to the configured monitoring system.
type Reporter interface {

	// Start reporting until the ctx cancelled. This method will block the current thread.
	Start(tomb *tomb.Tomb)
}

// NewReporter returns a Reporter instance.
func NewReporter() Reporter {
	return &pegasusReporter{
		reportInterval: viper.GetDuration("metrics.report_interval"),
	}
}

type pegasusReporter struct {
	reportInterval time.Duration
}

func (reporter *pegasusReporter) Start(tom *tomb.Tomb) {
	ticker := time.NewTicker(reporter.reportInterval)
	sink := newSink()
	aggregate.AddHookAfterTableStatEmitted(func(stats []aggregate.TableStats) {
		// periodically take snapshot of all the metrics and push them to the sink.
		var snapshots []*metricSnapshot
		for _, stat := range stats {
			for name, value := range stat.Stats {
				snapshots = append(snapshots, &metricSnapshot{
					name:  name,
					value: value,
					tags: map[string]string{
						"table":  stat.TableName,
						"entity": "table",
					},
				})
			}
		}
		go func() { // run asynchronously to avoid blocking in the hook
			sink.report(snapshots)
		}()
	})
	for {
		select {
		case <-tom.Dying(): // check if context cancelled
			return
		case <-ticker.C:
		}
	}
}
