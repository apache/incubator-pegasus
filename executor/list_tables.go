package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/olekukonko/tablewriter"
)

// ListTables command.
func ListTables(client *Client, file string, useJSON bool) error {
	if len(file) != 0 {
		save2File(client, file)
	} else {
		client.Writer = os.Stdout
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	resp, err := client.meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return err
	}
	/* TODO(jiashuo1) wait fix the err code
	if resp.Err != base.ERR_OK {
		return fmt.Errorf("Internal server error [%s]", resp.Err.String())
	}*/

	type tableStruct struct {
		Name string            `json:"name"`
		Envs map[string]string `json:"envs"`
	}
	var tbList []tableStruct
	for _, tb := range resp.Infos {
		tbList = append(tbList, tableStruct{
			Name: tb.AppName,
			Envs: tb.Envs,
		})
	}

	if useJSON {
		// formats into JSON
		outputBytes, err := json.MarshalIndent(tbList, "", "  ")
		if err != nil {
			return err
		}
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
