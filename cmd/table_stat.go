package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "table-stat",
		Help: "displays tables performance metrics",
		Run: func(c *grumble.Context) error {
			return executor.TableStat(pegasusClient)
		},
	})
}
