package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name:      "table-partitions",
		Help:      "show the table partition configuration in node",
		AllowArgs: true,
		Run: func(c *grumble.Context) error {
			var appName string
			if len(c.Args) == 1 {
				appName = c.Args[0]
			} else {
				return fmt.Errorf("Please input the table name")
			}

			return executor.ShowTablePartitions(pegasusClient, appName)
		},
	})
}
