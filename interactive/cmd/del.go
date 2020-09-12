package cmd

import (
	"fmt"
	"pegic/executor"
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	interactive.App.AddCommand(&grumble.Command{
		Name:  "del",
		Help:  "delete a record",
		Usage: "del <HASHKEY> <SORTKEY>",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 2 {
				return fmt.Errorf("invalid number (%d) of arguments for `del`", len(c.Args))
			}
			err := executor.Del(globalContext, c.Args[0], c.Args[1])
			if err != nil {
				return err
			}
			c.App.Println(globalContext)
			return nil
		},
		AllowArgs: true,
	})
}
