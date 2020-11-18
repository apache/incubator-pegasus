package cmd

import (
	"admin-cli/executor"
	"admin-cli/shell"
	"fmt"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name:    "duplication",
		Aliases: []string{"dup"},
		Help:    "duplication related control commands",
	}
	rootCmd.AddCommand(&grumble.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Help:    "list the duplications binding to the table",
		Run: func(c *grumble.Context) error {
			return executor.QueryDuplication(pegasusClient, useTable)
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add a duplications to the table",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("cluster") == "" {
				return fmt.Errorf("cluster cannot be empty")
			}
			return executor.AddDuplication(pegasusClient, useTable, c.Flags.String("cluster"), c.Flags.Bool("freezed"))
		},
		Flags: func(f *grumble.Flags) {
			f.String("c", "cluster", "", "the destination where the source data is duplicated")
			f.Bool("f", "freezed", false, "whether to freeze replica GC when duplication created")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:    "remove",
		Aliases: []string{"rm"},
		Help:    "remove a duplication from the table",
		Run: func(c *grumble.Context) error {
			if c.Flags.Int("dupid") == -1 {
				return fmt.Errorf("dupid cannot be empty")
			}
			return executor.ModifyDuplication(pegasusClient, useTable, c.Flags.Int("dupid"), admin.DuplicationStatus_DS_REMOVED)
		},
		Flags: func(f *grumble.Flags) {
			f.Int("d", "dupid", -1, "the dupid")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "pause",
		Help: "pause a duplication",
		Run: func(c *grumble.Context) error {
			if c.Flags.Int("dupid") == -1 {
				return fmt.Errorf("dupid cannot be empty")
			}
			return executor.ModifyDuplication(pegasusClient, useTable, c.Flags.Int("dupid"), admin.DuplicationStatus_DS_PAUSE)
		},
		Flags: func(f *grumble.Flags) {
			f.Int("d", "dupid", -1, "the dupid")
		},
	})
	shell.AddCommand(rootCmd)
}
