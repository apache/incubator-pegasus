package usage

import (
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v1"
)

// TableUsageRecorder records the usage of each table (in Capacity Unit)
// into a Pegasus table. The usage statistics can be used for service cost calculation.
type TableUsageRecorder interface {

	// Start recording until the ctx cancelled. This method will block the current thread.
	Start(tom *tomb.Tomb)
}

func NewTableUsageRecorder() TableUsageRecorder {
	return &tableUsageRecorder{
		usageStatFetchInterval: viper.GetDuration("usage_stat_fetch_interval"),
		usageStatApp:           viper.GetString("usage_stat_app"),
	}
}

type tableUsageRecorder struct {
	client pegasus.Client
	table  pegasus.TableConnector

	usageStatFetchInterval time.Duration
	usageStatApp           string
}

func (recorder *tableUsageRecorder) Start(tom *tomb.Tomb) {
	ticker := time.NewTicker(recorder.usageStatFetchInterval)
	for {
		select {
		case <-tom.Dying(): // check if context cancelled
			return
		case <-ticker.C:
			return
		default:
		}

		// periodically set/get a configured Pegasus table.
	}
}
