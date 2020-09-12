package cmd

import (
	"fmt"
	"pegic/executor"
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	interactive.App.AddCommand(&grumble.Command{
		Name:  "set",
		Help:  "write a record into Pegasus",
		Usage: "set <HASHKEY> <SORTKEY> <VALUE>",
		Run: requireUseTable(func(c *grumble.Context) error {
			if len(c.Args) != 3 {
				return fmt.Errorf("invalid number (%d) of arguments for `set`", len(c.Args))
			}
			err := executor.Set(globalContext, c.Args[0], c.Args[1], c.Args[2])
			if err != nil {
				return err
			}
			c.App.Println(globalContext)
			return nil
		}),
		AllowArgs: true,
	})
}
