package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "disk-migrate",
		Help: "migrate replica between the two disks within a specified ReplicaServer node",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.Bool("r", "resolve", false, "enable resolve node address, default false")
			f.String("g", "gpid", "", "gpid, for example, '2.1'")
			f.String("n", "node", "", "target node, for example, 127.0.0.1:34801")
			f.String("f", "from", "", "origin disk tag, for example, ssd1")
			f.String("t", "to", "", "target disk tag, for example, ssd2")
		},
		Run: func(c *grumble.Context) error {
			return executor.DiskMigrate(
				pegasusClient,
				c.Flags.String("node"),
				c.Flags.String("gpid"),
				c.Flags.String("from"),
				c.Flags.String("to"),
				c.Flags.Bool("resolve"))
		},
	})

	// TODO(jiashuo1) need generate migrate strategy(step) depends the disk-info result to run
	shell.AddCommand(&grumble.Command{
		Name: "disk-balance",
		Help: "migrate replica automatically to let the disk capacity balance within a specified ReplicaServer node",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
		},
		Run: func(c *grumble.Context) error {
			return executor.DiskBalance()
		},
	})

}
