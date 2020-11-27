package executor

import (
	"context"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"strconv"
	"time"
)

func RecallTable(client *Client, originTableId string, newTableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	tableId, errParse := strconv.ParseInt(originTableId, 10, 32)
	if errParse != nil {
		return errParse
	}

	resp, errRecall := client.Meta.RecallApp(ctx, &admin.RecallAppRequest{AppID: int32(tableId), NewAppName_: newTableName})
	if errRecall != nil {
		return errRecall
	}

	fmt.Printf("Recalling table \"%s\", ", resp.Info.AppName)
	errWait := waitTableReady(client, resp.Info.AppName, int(resp.Info.PartitionCount))

	if errWait != nil {
		return errWait
	}

	return nil
}
