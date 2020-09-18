package usage

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
)

type TableUsageRecorder interface {

	// Start recording until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

type tableUsageRecorderConfig struct {
	usageStatApp           string `"usage_stat_app"`
	usageStatFetchInterval int    `"usage_stat_fetch_interval"`
}

type tableUsageRecorder struct {
	client *pegasus.Client

	usageStatFetchInterval time.Duration
}

func (recorder *tableUsageRecorder) Start(ctx context.Context) error {
	ticker := time.NewTicker(recorder.usageStatFetchInterval)
	for {
		select {
		case <-ctx.Done(): // check if context cancelled
			return nil
		case <-ticker.C:
			return nil
		default:
		}

		// periodically set/get a configured Pegasus table.
	}
}
