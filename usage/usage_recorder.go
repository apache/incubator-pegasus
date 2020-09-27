package usage

import (
	"context"
	"fmt"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/pegasus-kv/collector/aggregate"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// TableUsageRecorder records the usage of each table into a Pegasus table.
// The usage statistics can be used for service cost calculation.
type TableUsageRecorder interface {

	// Start recording until the ctx cancelled. This method will block the current thread.
	Start(tom *tomb.Tomb)
}

// NewTableUsageRecorder returns an instance of TableUsageRecorder
func NewTableUsageRecorder() TableUsageRecorder {
	return &tableUsageRecorder{
		usageStatApp: viper.GetString("usage_stat_app"),
	}
}

type tableUsageRecorder struct {
	client pegasus.Client
	table  pegasus.TableConnector

	usageStatApp string
}

func (recorder *tableUsageRecorder) Start(tom *tomb.Tomb) {
	aggregate.AddHookAfterTableStatEmitted(func(stats []aggregate.TableStats, allStat aggregate.ClusterStats) {
		rootCtx := tom.Context(nil)
		for _, s := range stats {
			recorder.writeTableUsage(rootCtx, &s)
		}
	})
}

func (recorder *tableUsageRecorder) writeTableUsage(ctx context.Context, tb *aggregate.TableStats) {
	hashKey := []byte(fmt.Sprintf("%d", tb.Timestamp.Unix()))
	sortkey := []byte("cu")

	readCU := tb.Stats["recent_read_cu"]
	writeCU := tb.Stats["recent_write_cu"]
	value := []byte(fmt.Sprintf("{\"%d\":[%f, %f]}", tb.AppID, readCU, writeCU))

	go func() {
		maxRetryCount := 10
		for maxRetryCount > 0 {
			// TODO(wutao): set rpc timeout
			err := recorder.table.Set(ctx, hashKey, sortkey, value)
			if err == nil {
				break
			}
			log.Errorf("failed to write cu [timestamp: %s, appid: %d, readcu: %d, writecu: %d]",
				tb.Timestamp.Local().String(),
				tb.AppID,
				readCU,
				writeCU)
			maxRetryCount--
		}
	}()
}
