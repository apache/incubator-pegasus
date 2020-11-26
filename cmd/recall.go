package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name:    "recall",
		Help:    "recall the dropped table",
		Run: func(c *grumble.Context) error {
			var originTableId = ""
			var newTableName = ""
			if len(c.Args) == 1 {
				originTableId = c.Args[0]
			} else if len(c.Args) == 2 {
				originTableId = c.Args[0]
				newTableName = c.Args[1]
			} else {
				return fmt.Errorf("Invalid argument")
			}

			return executor.RecallTable(pegasusClient, originTableId, newTableName)
		},
	})
}
