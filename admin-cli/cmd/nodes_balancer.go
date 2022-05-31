package cmd

import (
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/nodesbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "nodes-balancer",
		Help: "migrate replica among the replica server to balance the capacity of cluster",
		Flags: func(a *grumble.Flags) {
			a.BoolL("auto", false, "whether to migrate replica until all nodes is balanced")
		},
		Run: func(c *grumble.Context) error {
			auto := c.Flags.Bool("auto")
			return nodesbalancer.BalanceNodeCapacity(pegasusClient, auto)
		},
	})
}
