package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"

	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "app-env",
		Help: "app-env related commands",
	}
	rootCmd.AddCommand(&grumble.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Help:    "list the app-envs binding to the table",
		Run: func(c *grumble.Context) error {
			return executor.ListAppEnvs(pegasusClient, useTable)
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "set",
		Help: "set an env with key and value",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 2 {
				return fmt.Errorf("invalid number (%d) of arguments for `app-env set`", len(c.Args))
			}
			return executor.SetAppEnv(pegasusClient, useTable, c.Args[0], c.Args[1])
		},
		AllowArgs: true,
	})
	shell.AddCommand(rootCmd)
}
