package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"

	"github.com/desertbit/grumble"
)

var predefinedMetaLevel = []string{
	"lively",
	"steady",
}

func init() {
	rootCmd := &grumble.Command{
		Name: "meta-level",
		Help: "Get the current meta function level",
		Run: func(c *grumble.Context) error {
			return executor.GetMetaLevel(pegasusClient)
		},
	}
	rootCmd.AddCommand(
		&grumble.Command{
			Name: "set",
			Help: "reset the meta function level",
			Run: func(c *grumble.Context) error {
				if len(c.Args) != 1 {
					return fmt.Errorf("invalid number (%d) of arguments for `meta-level set`", len(c.Args))
				}
				return executor.SetMetaLevel(pegasusClient, c.Args[0])
			},
			Completer: func(prefix string, args []string) []string {
				if len(args) == 0 {
					return filterStringWithPrefix(predefinedMetaLevel, prefix)
				}
				return []string{}
			},
			AllowArgs: true,
		},
	)
	shell.App.AddCommand(rootCmd)
}
