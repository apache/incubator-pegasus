package cmd

import (
	"admin-cli/executor"
	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "remote-command",
		Help: "send remote command, for example, remote-command meta or replica",
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:  "meta",
		Help:  "send remote command to meta server",
		Flags: initFlag,
		Run: func(c *grumble.Context) error {
			return executor.RemoteCommand(pegasusClient, "meta", c.Flags.String("node"), c.Flags.String("command"), c.Flags.String("arguments"))
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "replica",
		Help:  "send remote command to replica server",
		Flags: initFlag,
		Run: func(c *grumble.Context) error {
			return executor.RemoteCommand(pegasusClient, "replica", c.Flags.String("node"), c.Flags.String("command"), c.Flags.String("arguments"))
		},
	})
}

func initFlag(f *grumble.Flags) {
	/*define the flags*/
	f.String("n", "node", "", "specify server node address, such as 127.0.0.1:34801, empty mean all node")
	f.String("c", "command", "help", "remote command name, you can -c help to see support command")
	f.String("a", "arguments", "", "if empty means query the command argument value, otherwise mean set update value")
}
