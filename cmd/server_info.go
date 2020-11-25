package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "server-info",
		Help: "displays the overall server information",
		Run: func(c *grumble.Context) error {
			return executor.ServerInfo(pegasusClient)
		},
	})

}
