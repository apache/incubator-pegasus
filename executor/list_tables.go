package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
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

	type tableStuct struct {
		Name string            `json:"name"`
		Envs map[string]string `json:"envs"`
	}
	var tbList []tableStuct
	for _, tb := range resp.Infos {
		tbList = append(tbList, tableStuct{
			Name: tb.AppName,
			Envs: tb.Envs,
		})
	}

	if useJSON {
		// formats into JSON
		outputBytes, _ := json.MarshalIndent(tbList, "", "  ")
		fmt.Fprintln(client, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(client)
	tabular.SetHeader([]string{"Name", "Envs"})
	for _, tb := range tbList {
		tabular.Append([]string{tb.Name, fmt.Sprintf("%s", tb.Envs)})
	}
	tabular.Render()
	return nil
}
