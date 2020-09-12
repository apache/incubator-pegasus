package cmd

import (
	"fmt"
	"pegic/executor"
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	interactive.App.AddCommand(&grumble.Command{
		Name:  "get",
		Help:  "read a record from Pegasus",
		Usage: "get <HASHKEY> <SORTKEY>",
		Run: requireUseTable(func(c *grumble.Context) error {
			if len(c.Args) != 2 {
				return fmt.Errorf("invalid number (%d) of arguments for `get`", len(c.Args))
			}
			err := executor.Get(globalContext, c.Args[0], c.Args[1])
			if err != nil {
				return err
			}
			c.App.Println(globalContext)
			return nil
		}),
		AllowArgs: true,
	})
}
