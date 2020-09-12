package interactive

import (
	"github.com/desertbit/grumble"
)

// App is the global interactive application.
var App *grumble.App

func init() {
	App = grumble.New(&grumble.Config{
		Name:        "pegic",
		HistoryFile: ".pegic-history",
	})
}

func Run() {
	App.Run()
}
