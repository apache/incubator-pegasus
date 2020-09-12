package shell

import (
	"pegic/interactive"
	"pegic/interactive/cmd"
	"strings"

	"github.com/spf13/cobra"
)

// Root command for pegic.
var Root *cobra.Command

func init() {
	Root = &cobra.Command{
		Use:   "pegic [--meta|-m <meta-list>]",
		Short: "pegic: Pegasus Interactive Command-Line tool",
		PreRun: func(c *cobra.Command, args []string) {
			// validate meta-list
			ms := c.Flags().StringP("meta", "m", "127.0.0.1:34601,127.0.0.1:34602", "the list of MetaServer addresses")
			metaAddrs := strings.Split(*ms, ",")
			cmd.Init(metaAddrs)
		},
		Run: func(cmd *cobra.Command, args []string) {
			// the default entrance is interactive
			interactive.Run()
		},
	}
}
