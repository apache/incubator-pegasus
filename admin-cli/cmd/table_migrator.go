package cmd

import (
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/tablemigrator"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "table-migrator",
		Help: "migrate table from current cluster to another via table duplication and metaproxy",
		Run: func(c *grumble.Context) error {
			return tablemigrator.MigrateTable(pegasusClient, c.Flags.String("table"),
				c.Flags.String("node"), c.Flags.String("root"),
				c.Flags.String("cluster"), c.Flags.String("meta"))
		},
		Flags: func(f *grumble.Flags) {
			f.String("t", "table", "", "table name")
			f.String("n", "node", "", "zk node: addrs:port, default equal with peagsus "+
				"cluster zk addrs, you can use `cluster-info` to show it")
			f.String("r", "root", "", "zk root path. the tool will update table addrs in "+
				"the path of meatproxy, if you don't specify it, that is means user need manual-switch the table addrs")
			f.String("c", "cluster", "", "target cluster name")
			f.String("m", "meta", "", "target meta list")
		},
	})
}
