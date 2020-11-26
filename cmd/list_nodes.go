package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name:    "list-nodes",
		Aliases: []string{"nodes"},
		Help:    "list all nodes in the cluster",
		Flags: func(f *grumble.Flags) {
			f.Bool("d", "detail", false, "show detail node info in all node")
		},
		Run: func(c *grumble.Context) error {
			return executor.ListNodes(
				pegasusClient,
				c.Flags.Bool("detail"),
			)
		},
	})
}
