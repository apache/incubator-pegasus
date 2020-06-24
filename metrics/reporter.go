package metrics

import (
	"context"
	"time"
)

// Reporter reports metrics of this Collector to the configured monitoring system.
type Reporter interface {

	// Start reporting until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

func NewReporter() Reporter {
	return &pegasusReporter{}
}

type pegasusReporter struct {
	reportInterval time.Duration
}

func (reporter *pegasusReporter) Start(ctx context.Context) error {
	ticker := time.NewTicker(reporter.reportInterval)
	for {
		select {
		case <-ctx.Done(): // check if context cancelled
			return nil
		case <-ticker.C:
			return nil
		default:
		}

		// periodically take snapshot of all the metrics and push them to the sink.
	}

}
