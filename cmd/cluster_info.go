package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "cluster-info",
		Help: "displays the overall cluster information",
		Run: func(c *grumble.Context) error {
			return executor.ClusterInfo(pegasusClient)
		},
	})
}
