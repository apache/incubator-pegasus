package metrics

import (
	"time"

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
	sink := NewSink()
	for {
		select {
		case <-tom.Dying(): // check if context cancelled
			return
		case <-ticker.C:
		}

		// periodically take snapshot of all the metrics and push them to the sink.
		sink.Report()
	}
}
