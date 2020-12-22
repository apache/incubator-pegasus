package cmd

import (
	"fmt"
	"pegic/executor"
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name:  "partition-index",
		Help:  "ADVANCED: calculate the partition index where the hashkey is routed to",
		Usage: "partition-index <HASHKEY>",
		Run: requireUseTable(func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("invalid number (%d) of arguments for `partition-index`", len(c.Args))
			}
			err := executor.PartitionIndex(globalContext, c.Args[0])
			if err != nil {
				return err
			}
			c.App.Println(globalContext)
			return nil
		}),
		AllowArgs: true,
	}
	interactive.App.AddCommand(rootCmd)
}
