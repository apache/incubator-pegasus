package cmd

import (
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "data-version",
		Help: "query data version",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			return executor.QueryTableVersion(pegasusClient, c.UseTable)
		}),
	})
}
