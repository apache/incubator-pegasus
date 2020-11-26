package executor

import (
	"context"
	"time"
)

// GetMetaLevel command
func RecallTable(client *Client, originTableId string, newTableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	return nil
}
