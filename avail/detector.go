package avail

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
)

// Detector periodically checks the availability of the remote Pegasus cluster.
type Detector interface {

	// Start detection until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

// NewDetector returns a Detector.
func NewDetector(client *pegasus.Client) Detector {
	return &pegasusDetector{client: client}
}

type pegasusDetector struct {
	client      *pegasus.Client
	detectTable *pegasus.TableConnector

	detectInterval  time.Duration
	detectTableName string
	detectTimeout   time.Duration
}

func (d *pegasusDetector) Start(rootCtx context.Context) error {
	var err error
	ctx, _ := context.WithTimeout(rootCtx, d.detectTimeout)
	d.detectTable, err = d.client.OpenTable(ctx, d.detectTableName)

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
