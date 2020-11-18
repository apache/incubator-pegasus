package cmd

import (
	"admin-cli/shell"
	"fmt"

	"github.com/desertbit/grumble"
)

var useTable string

func init() {
	shell.AddCommand(&grumble.Command{
		Name:  "use",
		Help:  "select a table",
		Usage: "use <TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				c.App.Println(c.Command.Usage)
				return fmt.Errorf("invalid number (%d) of arguments for `use`", len(c.Args))
			}
			useTable = c.Args[0]
			// TODO(wutao): verify if the use table exists
			c.App.Println("ok")
			return nil
		},
		AllowArgs: true,
		Completer: func(prefix string, args []string) []string {
			// TODO(wutao): auto-completion of tables
			return []string{}
		},
	})
}
