package avail

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	log "github.com/sirupsen/logrus"
)

// Detector periodically checks the availability of the remote Pegasus cluster.
type Detector interface {

	// Start detection until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

// NewDetector returns a detector of service availability.
func NewDetector(client pegasus.Client) Detector {
	return &pegasusDetector{client: client}
}

type pegasusDetector struct {
	client      pegasus.Client
	detectTable pegasus.TableConnector

	detectInterval  time.Duration
	detectTableName string

	// timeout of a single detect
	detectTimeout time.Duration

	detectHashKeys [][]byte

	recentMinuteDetectTimes  uint64
	recentMinuteFailureTimes uint64

	recentHourDetectTimes  uint64
	recentHourFailureTimes uint64

	recentDayDetectTimes  uint64
	recentDayFailureTimes uint64
}

func (d *pegasusDetector) Start(rootCtx context.Context) error {
	var err error
	ctx, _ := context.WithTimeout(rootCtx, 10*time.Second)
	d.detectTable, err = d.client.OpenTable(ctx, d.detectTableName)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(d.detectInterval)
	for {
		select {
		case <-rootCtx.Done(): // check if context cancelled
			return nil
		case <-ticker.C:
			return nil
		default:
		}

		// periodically set/get a configured Pegasus table.

	}
}

func (d *pegasusDetector) generateHashKeys() {

}

func (d *pegasusDetector) writeResult() {

}

func (d *pegasusDetector) detectPartition(rootCtx context.Context, partitionIdx int) {
	d.incrDetectTimes()

	go func() {
		ctx, _ := context.WithTimeout(rootCtx, d.detectTimeout)

		hashkey := d.detectHashKeys[partitionIdx]
		value := []byte("")

		if err := d.detectTable.Set(ctx, hashkey, []byte(""), value); err != nil {
			d.incrFailureTimes()
			log.Errorf("set partition [%d] failed, hashkey=\"%s\": %s", partitionIdx, err)
		}
		if _, err := d.detectTable.Get(ctx, hashkey, []byte("")); err != nil {
			d.incrFailureTimes()
			log.Errorf("get partition [%d] failed, hashkey=\"%s\": %s", partitionIdx, err)
		}
	}()
}

func (d *pegasusDetector) incrDetectTimes() {
	atomic.AddUint64(&d.recentMinuteDetectTimes, 1)
	atomic.AddUint64(&d.recentHourDetectTimes, 1)
	atomic.AddUint64(&d.recentDayDetectTimes, 1)
}

func (d *pegasusDetector) incrFailureTimes() {
	atomic.AddUint64(&d.recentMinuteFailureTimes, 1)
	atomic.AddUint64(&d.recentHourFailureTimes, 1)
	atomic.AddUint64(&d.recentDayFailureTimes, 1)
}
