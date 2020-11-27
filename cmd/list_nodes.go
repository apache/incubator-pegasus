package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "nodes",
		Help: "query all nodes perf stat in the cluster",
		Flags: func(f *grumble.Flags) {
			f.String("t", "table", "", "show one app replica info in cluster")
		},
		Run: func(c *grumble.Context) error {
			return executor.ListNodes(
				pegasusClient,
				c.Flags.String("table"),
			)
		},
	})
}
