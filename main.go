package main

import (
	"admin-cli/cmd"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func main() {
	cmd.Init()
	grumble.Main(shell.App)
}
