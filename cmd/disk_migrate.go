package cmd

import (
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "disk-migrate",
		Help: "migrate replica between the two disks within a specified ReplicaServer node",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
		},
		Run: func(c *grumble.Context) error {
			return diskMigrate(c)
		},
	})
}

func diskMigrate(c *grumble.Context) error {
	/*c.Flags["flag1"]*/
	return nil
}
