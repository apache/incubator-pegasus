package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	// TODO(jiashuo1) support query detail info
	shell.AddCommand(&grumble.Command{
		Name:    "list-nodes",
		Aliases: []string{"nodes"},
		Help:    "list all nodes in the cluster",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("j", "json", false, "Use JSON as the format of the output results. By default tabular format is used.")
		},
		Run: func(c *grumble.Context) error {
			return executor.ListNodes(pegasusClient, c.Flags.Bool("json"))
		},
	})
}
