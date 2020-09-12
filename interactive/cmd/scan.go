package cmd

import (
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	usage := `
<hashKey>
[
  BETWEEN <startSortKey> AND <stopSortKey>
  PREFIX <filter>
  SUFFIX <filter>
  CONTAINS <filter>
]`

	interactive.App.AddCommand(&grumble.Command{
		Name:      "scan",
		Help:      "scan records under the hashkey",
		Usage:     usage,
		Run:       requireUseTable(parseScanCommand),
		AllowArgs: true,
	})
}

func parseScanCommand(c *grumble.Context) error {
	return nil
}
