package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "disk-capacity",
		Help: "query disk capacity info ",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("r", "resolve", false, "resolve input or output address, default false")
			f.Bool("j", "json", false, "use JSON as the format of the output results. By default tabular format is used.")
			f.String("n", "node", "", "node address(ip:port), for example, 127.0.0.1:34801")
			f.String("d", "disk", "", "disk tag, for example, ssd1")
			f.String("a", "app", "", "app name, for example, temp")
		},
		Run: func(c *grumble.Context) error {
			return executor.QueryDiskInfo(
				pegasusClient,
				executor.CapacitySize,
				c.Flags.String("node"),
				c.Flags.String("app"),
				c.Flags.String("disk"),
				c.Flags.Bool("json"),
				c.Flags.Bool("resolve"))
		},
	})

	shell.AddCommand(&grumble.Command{
		Name: "disk-replica",
		Help: "query disk replica count info",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("r", "resolve", false, "resolve input or output address, default false")
			f.Bool("j", "json", false, "use JSON as the format of the output results. By default tabular format is used.")
			f.String("n", "node", "", "node address(ip:port), for example, 127.0.0.1:34801")
			f.String("a", "app", "", "app name, for example, temp")
		},
		Run: func(c *grumble.Context) error {
			return executor.QueryDiskInfo(
				pegasusClient,
				executor.ReplicaCount,
				c.Flags.String("node"),
				c.Flags.String("app"),
				"",
				c.Flags.Bool("json"),
				c.Flags.Bool("resolve"))
		},
	})
}
