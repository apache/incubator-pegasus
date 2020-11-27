package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "nodes-stat",
		Help: "query all nodes perf stat in the cluster",
		Flags: func(f *grumble.Flags) {
			f.Bool("d", "detail", false, "show node detail perf stats in cluster")
		},
		Run: func(c *grumble.Context) error {
			return executor.ShowNodesStat(
				pegasusClient,
				c.Flags.Bool("detail"),
			)
		},
	})
}
