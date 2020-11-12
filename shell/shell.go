package shell

import (
	"github.com/desertbit/grumble"
)

// App is the global shell app.
var App *grumble.App

// AddCommand registers the command to the global shell app.
func AddCommand(cmd *grumble.Command) {
	App.AddCommand(cmd)
}

func init() {
	App = grumble.New(&grumble.Config{
		Name:        "admin-cli",
		Description: "Pegasus administration command line tool",
		Flags: func(f *grumble.Flags) {
			f.String("m", "meta", "127.0.0.1:34601,127.0.0.1:34602", "a list of MetaServer IP:Port addresses")
		},
		HistoryFile: ".admin-cli-history",
	})
}
