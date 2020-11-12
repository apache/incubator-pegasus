package main

import (
	"admin-cli/cmd"
	"admin-cli/shell"
	"fmt"
	"net"
	"strings"

	"github.com/desertbit/grumble"
)

func main() {
	shell.App.OnInit(func(a *grumble.App, flags grumble.FlagMap) error {
		metaListStr := flags.String("meta")
		metaList := strings.Split(metaListStr, ",")
		for _, metaIPPort := range metaList {
			_, err := net.ResolveTCPAddr("tcp4", metaIPPort)
			if err != nil {
				return fmt.Errorf("Invalid MetaServer TCP address [%s]", err)
			}
		}
		cmd.Init(metaList)
		return nil
	})
	grumble.Main(shell.App)
}
