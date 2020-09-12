package shell

import (
	"github.com/spf13/cobra"
	"pegic/executor/util"
)

func init() {
	// flags
	var from *string
	var to *string

	cmd := &cobra.Command{
		Use:   "convert",
		Short: "convert bytes from one encoding to the other",
		Args:  cobra.ExactArgs(1),
		Run: func(c *cobra.Command, args []string) {
			fromEnc := util.NewEncoder(*from)
			if fromEnc == nil {
				c.PrintErrf("invalid encoding: \"%s\"", *from)
				return
			}
			toEnc := util.NewEncoder(*to)
			if toEnc == nil {
				c.PrintErrf("invalid encoding: \"%s\"", *to)
				return
			}

			data, err := fromEnc.EncodeAll(args[0])
			if err != nil {
				c.PrintErr(err)
				return
			}
			result, err := toEnc.DecodeAll(data)
			if err != nil {
				c.PrintErr(err)
				return
			}
			c.Println(result)
		},
	}

	from = cmd.Flags().String("from", "", "the original encoding of the given bytes")
	to = cmd.Flags().String("to", "", "the original encoding of the given bytes")
	_ = cmd.MarkFlagRequired("from")
	_ = cmd.MarkFlagRequired("to")

	Root.AddCommand(cmd)
}
