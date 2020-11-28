package cmd

import (
	"admin-cli/shell"
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/desertbit/grumble"
)

var useTable string

var cachedTableNames []string

// whether table names are previously cached
var tablesCached bool = false

func init() {
	shell.AddCommand(&grumble.Command{
		Name:  "use",
		Help:  "select a table",
		Usage: "use <TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				c.App.Println(c.Command.Usage)
				return fmt.Errorf("invalid number (%d) of arguments for `use`", len(c.Args))
			}
			useTable = c.Args[0]
			// TODO(wutao): verify if the use table exists
			c.App.Println("ok")
			return nil
		},
		AllowArgs: true,
		Completer: func(prefix string, args []string) []string {
			return useCompletion(prefix)
		},
	})
}

func useCompletion(prefix string) []string {
	if tablesCached {
		// returns all tables
		return cachedTableNames
	}

	// TODO(wutao): auto-completes in background

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := pegasusClient.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return []string{}
	}

	tableNames := []string{}
	for _, app := range resp.Infos {
		tableNames = append(tableNames, app.AppName)
	}
	cachedTableNames = tableNames
	tablesCached = true
	return tableNames
}
