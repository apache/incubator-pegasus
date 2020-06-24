package usage

import (
	"context"

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
}
