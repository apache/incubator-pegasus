package executor

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/olekukonko/tablewriter"
)

// ListTables command.
func ListTables(client admin.Client, useJSON bool) error {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
	tables, err := client.ListTables(ctx)
	if err != nil {
		return err
	}

	tabular := tablewriter.NewWriter(os.Stdout)
	tabular.SetHeader([]string{"Name", "Envs"})
	for _, tb := range tables {
		tabular.Append([]string{tb.Name, fmt.Sprintf("%s", tb.Envs)})
	}
	tabular.Render()
	return nil
}
