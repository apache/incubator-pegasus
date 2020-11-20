package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "meta-level",
		Help: "Get the current meta function level",
		Run: func(c *grumble.Context) error {
			return executor.GetMetaLevel(pegasusClient)
		},
	}
	shell.App.AddCommand(rootCmd)
}
