package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/olekukonko/tablewriter"
)

// ListTables command.
func ListTables(writer io.Writer, client admin.Client, useJSON bool) error {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	tables, err := client.ListTables(ctx)
	if err != nil {
		return err
	}

	if useJSON {
		// formats into JSON
		type tableJSON struct {
			Name string            `json:"name"`
			Envs map[string]string `json:"envs"`
		}
		var tbJSONList []tableJSON
		for _, tb := range tables {
			tbJSONList = append(tbJSONList, tableJSON{Name: tb.Name, Envs: tb.Envs})
		}
		outputBytes, err := json.MarshalIndent(tbJSONList, "", "  ")
		if err != nil {
			return err
		}
		fmt.Fprintln(writer, string(outputBytes))
		return nil
	}

	// formats into tabular
	tabular := tablewriter.NewWriter(writer)
	tabular.SetHeader([]string{"Name", "Envs"})
	for _, tb := range tables {
		tabular.Append([]string{tb.Name, fmt.Sprintf("%s", tb.Envs)})
	}
	tabular.Render()
	return nil
}
