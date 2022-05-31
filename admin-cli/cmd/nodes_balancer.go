package cmd

import (
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/nodesbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "nodes-balancer",
		Help: "migrate replica among the replica server to balance the capacity of cluster, please " +
			// todo(jiashuo)  update the link of master after merging into master
			"make sure the server config is right, detail see https://..",
		Flags: func(a *grumble.Flags) {
			a.BoolL("auto", false, "whether to migrate replica until all nodes is balanced, false "+
				"by default, which means it just migrate one replica")
		},
		Run: func(c *grumble.Context) error {
			auto := c.Flags.Bool("auto")
			return nodesbalancer.BalanceNodeCapacity(pegasusClient, auto)
		},
	})
}
