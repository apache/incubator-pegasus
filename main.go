package main

import (
	"admin-cli/cmd"
	"admin-cli/shell"
	"fmt"
	"net"
	"strings"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/desertbit/grumble"
	"github.com/sirupsen/logrus"
	"gopkg.in/natefinch/lumberjack.v2"
)


func main() {
	// pegasus-go-client's logs use the same logger as admin-cli.
	pegalog.SetLogger(logrus.StandardLogger())
	// configure log destination
	logrus.SetOutput(&lumberjack.Logger{
		Filename:  "./shell.log",
		LocalTime: true,
	})

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
