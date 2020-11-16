package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "cluster-info",
		Help: "list all tables in the cluster",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("j", "json", false, "Use JSON as the format of the output results. By default tabular format is used.")
		},
		Run: func(c *grumble.Context) error {
			return executor.ClusterInfo(pegasusClient, c.Flags.Bool("json"))
		},
	})
}
