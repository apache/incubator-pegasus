package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "create",
		Help: "create a table",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("must specify a table name")
			}
			return executor.CreateTable(pegasusClient, c.Args[0], c.Flags.Int("partitions"))
		},
		Flags: func(f *grumble.Flags) {
			f.Int("p", "partitions", 4, "the number of partitions")
		},
		AllowArgs: true,
	})
}
