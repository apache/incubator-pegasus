package interactive

import (
	"fmt"
	"os"

	"github.com/desertbit/grumble"
)

// App is the global interactive application.
var App *grumble.App

func init() {
	App = grumble.New(&grumble.Config{
		Name:        "pegic",
		HistoryFile: ".pegic-history",
		Flags: func(f *grumble.Flags) {
			f.String("m", "meta", "127.0.0.1:34601,127.0.0.1:34602", "the list of MetaServer addresses")
		},
	})
}

func Run() {
	err := App.Run()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
