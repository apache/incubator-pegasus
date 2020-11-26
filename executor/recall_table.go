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

	appId, errParse := strconv.ParseInt(originTableId, 10, 32)
	if errParse != nil {
		return errParse
	}

	resp, errRecall := client.Meta.RecallApp(ctx, &admin.RecallAppRequest{AppID: int32(appId), NewAppName_: newTableName})
	if errRecall != nil {
		return errRecall
	}

	fmt.Printf("Recalling table \"%s\", ", resp.Info.AppName)
	errWait := WaitTableReady(client, resp.Info.AppName, int(resp.Info.PartitionCount))

	if errWait != nil {
		return errWait
	}

	return nil
}
