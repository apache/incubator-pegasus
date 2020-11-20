package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

// TODO(jiashuo1) support show node replica/qps/latency/usage info(need remote-command support)
func init() {
	shell.AddCommand(&grumble.Command{
		Name:    "list-nodes",
		Aliases: []string{"nodes"},
		Help:    "list all nodes in the cluster",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("r", "resolve", false, "resolve input or output address")
			f.Bool("j", "json", false, "use JSON as the format of the output results. By default tabular format is used.")
			f.Bool("d", "detail", false, "show detail replica count in all node")
			f.Bool("u", "usage", false, "show the usage of every node")
			f.Bool("q", "qps", false, "show the qps/bytes/latency of every node")
			f.String("a", "app", "", "filter one app, for example, temp")
			f.String("o", "out", "", "save output into file")
		},
		Run: func(c *grumble.Context) error {
			return executor.ListNodes(
				pegasusClient,
				c.Flags.String("app"),
				c.Flags.Bool("detail"),
				c.Flags.Bool("usage"),
				c.Flags.Bool("qps"),
				c.Flags.Bool("json"),
				c.Flags.Bool("resolve"))
		},
	})
}
