package shell

import (
	"os"
	"pegic/interactive"
	"pegic/interactive/cmd"
	"strings"

	"github.com/spf13/cobra"
)

// Root command for pegic.
var Root *cobra.Command

func init() {
	var metaList *string

	Root = &cobra.Command{
		Use:   "pegic [--meta|-m <meta-list>]",
		Short: "pegic: Pegasus Interactive Command-Line tool",
		PreRun: func(c *cobra.Command, args []string) {
			metaAddrs := strings.Split(*metaList, ",")
			err := cmd.Init(metaAddrs)
			if err != nil {
				c.PrintErrln(err)
				os.Exit(1)
			}
		},
		Run: func(cmd *cobra.Command, args []string) {
			// the default entrance is interactive
			interactive.Run()
		},
	}

	metaList = Root.Flags().StringP("meta", "m", "127.0.0.1:34601,127.0.0.1:34602", "the list of MetaServer addresses")
}
