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
		Flags: func(f *grumble.Flags) {
			f.Bool("r", "resolve", false, "resolve input or output address")
		},
		Run: func(c *grumble.Context) error {
			return executor.ServerInfo(pegasusClient, c.Flags.Bool("resolve"))
		},
	})

}
