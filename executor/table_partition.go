package executor

import (
	"context"
	"time"
)

// GetMetaLevel command
func TablePartition(client *Client, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	return nil
}
