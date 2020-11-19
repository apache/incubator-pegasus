package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// CreateTable command
func CreateTable(c *Client, tableName string, partitionCount int) error {
	if partitionCount < 1 {
		return fmt.Errorf("partitions count should >=1")
	}
	// TODO(wutao): reject request with invalid table name

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := c.Meta.CreateApp(ctx, &admin.CreateAppRequest{
		AppName: tableName,
		Options: &admin.CreateAppOptions{
			PartitionCount: int32(partitionCount),
			IsStateful:     true,
			AppType:        "pegasus",
			SuccessIfExist: true,
			ReplicaCount:   3,
		},
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(c, "Creating table \"%s\" [AppID: %d]\n", tableName, resp.Appid)
	return nil
}
