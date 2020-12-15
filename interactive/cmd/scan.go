package cmd

import (
	"fmt"
	"pegic/executor"
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	usage := `<hashKey>
[ --from <startSortKey> ]
[ --to <stopSortKey> ]
[
  --prefix <filter>
  --suffix <filter>
  --contains <filter>
]`

	scanCmd := &grumble.Command{
		Name:  "scan",
		Help:  "scan records under the hashkey",
		Usage: "\nscan " + usage,
		Run: requireUseTable(func(c *grumble.Context) error {
			cmd, err := newScanCommand(c)
			if err != nil {
				return err
			}
			return cmd.IterateAll(globalContext)
		}),
		Flags:     scanFlags,
		AllowArgs: true,
	}

	scanCmd.AddCommand(&grumble.Command{
		Name:  "count",
		Help:  "scan records under the hashkey",
		Usage: "\nscan " + usage,
		Run: requireUseTable(func(c *grumble.Context) error {
			cmd, err := newScanCommand(c)
			if err != nil {
				return err
			}
			cmd.CountOnly = true
			return cmd.IterateAll(globalContext)
		}),
		Flags:     scanFlags,
		AllowArgs: true,
	})

	interactive.App.AddCommand(scanCmd)
}

func newScanCommand(c *grumble.Context) (*executor.ScanCommand, error) {
	if len(c.Args) < 1 {
		return nil, fmt.Errorf("missing <hashkey> for `scan`")
	}

	from := c.Flags.String("from")
	to := c.Flags.String("to")
	suffix := c.Flags.String("suffix")
	prefix := c.Flags.String("prefix")
	contains := c.Flags.String("contains")

	cmd := &executor.ScanCommand{HashKey: c.Args[0]}
	if from != "" {
		cmd.From = &from
	}
	if to != "" {
		cmd.To = &to
	}
	if suffix != "" {
		cmd.Suffix = &suffix
	}
	if prefix != "" {
		cmd.Prefix = &prefix
	}
	if contains != "" {
		cmd.Contains = &contains
	}
	if err := cmd.Validate(); err != nil {
		return nil, err
	}
	return cmd, nil
}

func scanFlags(f *grumble.Flags) {
	f.StringL("from", "", "<startSortKey>")
	f.StringL("to", "", "<stopSortKey>")
	f.StringL("prefix", "", "<filter>")
	f.StringL("suffix", "", "<filter>")
	f.StringL("contains", "", "<filter>")
}
