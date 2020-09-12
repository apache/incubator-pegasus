package cmd

import (
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "compression",
		Help: "read the current compression algorithm",
		Run: func(c *grumble.Context) error {
			// TODO(wutao): verify if the use table exists
			c.App.Println("ok")
			return nil
		},
		AllowArgs: true,
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:    "set",
		Aliases: []string{"SET"},
		Help:    "reset the compression algorithm, default no",
		Run: func(c *grumble.Context) error {
			c.App.Println("ok")

			// TODO: require use table

			return nil
		},
		AllowArgs: true,
	})

	interactive.App.AddCommand(rootCmd)
}
