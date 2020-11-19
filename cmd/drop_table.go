package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "drop",
		Help: "drop a table",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("must specify a table name")
			}
			return executor.DropTable(pegasusClient, c.Args[0], c.Flags.Duration("reserved"))
		},
		Flags: func(f *grumble.Flags) {
			f.Duration("r", "reserved", 4, "the soft-deletion period, which is the time before table actually deleted")
		},
		AllowArgs: true,
	})
}
