package pegasus

import (
	"time"
	"context"
)

type ScannerOptions struct {
	Timeout time.Duration

	BatchSize      int
	StartInclusive bool
	StopInclusive  bool
	HashKeyFilter  Filter
	SortKeyFilter  Filter
}

type Scanner interface {
	next(ctx context.Context) (err error, hashKey []byte, sortKey []byte, value []byte)
}
