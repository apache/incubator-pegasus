package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name:      "app",
		Help:      "show the app partition distribution in node",
		AllowArgs: true,
		Run: func(c *grumble.Context) error {
			var appName = ""
			if len(c.Args) == 1 {
				appName = c.Args[0]
			} else {
				return fmt.Errorf("Please input the app name")
			}

			return executor.TablePartition(pegasusClient, appName)
		},
	})
}
