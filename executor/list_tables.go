package executor

import (
	"admin-cli/tabular"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
)

// ListTables command.
func ListTables(client *Client, useJSON bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return err
	}

	type tableStruct struct {
		AppID          int32             `json:"app_id"`
		Name           string            `json:"name"`
		PartitionCount int32             `json:"partition_count"`
		CreateTime     string            `json:"create_time"`
		Envs           map[string]string `json:"envs"`
	}
	var tbList []interface{}
	for _, tb := range resp.Infos {
		tbList = append(tbList, tableStruct{
			AppID:          tb.AppID,
			Name:           tb.AppName,
			PartitionCount: *&tb.PartitionCount,
			CreateTime:     time.Unix(tb.CreateSecond, 0).Format("2006-01-02"),
			Envs:           tb.Envs,
		})
	}

	if useJSON {
		// formats into JSON
		outputBytes, _ := json.MarshalIndent(tbList, "", "  ")
		fmt.Fprintln(client, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular.Print(client, tbList)
	return nil
}
