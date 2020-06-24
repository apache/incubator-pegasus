package metrics

import (
	"context"
)

// Reporter reports metrics of this Collector to the configured monitoring system.
type Reporter interface {

	// Start reporting until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

func NewReporter() Reporter {

}

type pegasusReporter struct {
}
