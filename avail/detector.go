package avail

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
)

// Detector periodically checks the availability of the remote cluster.
type Detector interface {

	// Start detection until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

// NewDetector returns a Detector.
func NewDetector(client *pegasus.Client) (Detector, error) {
	return &pegasusDetector{}, nil
}

type pegasusDetector struct {
	client *pegasus.Client

	detectInterval time.Duration
}

func (d *pegasusDetector) Start(ctx context.Context) error {
	ticker := time.NewTicker(d.detectInterval)
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
