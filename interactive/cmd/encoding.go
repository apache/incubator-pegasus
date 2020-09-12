package cmd

import (
	"fmt"
	"pegic/executor/util"
	"pegic/interactive"

	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "encoding",
		Help: "read the current encoding",
		Run: func(c *grumble.Context) error {
			c.App.Println(globalContext)
			return nil
		},
	}

	rootCmd.AddCommand(&grumble.Command{
		Name: "hashkey",
		Help: "set encoding for hashkey",
		Run: func(c *grumble.Context) error {
			return resetEncoding(c, &globalContext.HashKeyEnc)
		},
		AllowArgs: true,
		Completer: encodingCompleter,
	})

	rootCmd.AddCommand(&grumble.Command{
		Name: "sortkey",
		Help: "set encoding for sortkey",
		Run: func(c *grumble.Context) error {
			return resetEncoding(c, &globalContext.SortKeyEnc)
		},
		AllowArgs: true,
		Completer: encodingCompleter,
	})

	rootCmd.AddCommand(&grumble.Command{
		Name: "value",
		Help: "set encoding for value",
		Run: func(c *grumble.Context) error {
			return resetEncoding(c, &globalContext.ValueEnc)
		},
		AllowArgs: true,
		Completer: encodingCompleter,
	})

	interactive.App.AddCommand(rootCmd)
}

// resetEncoding is the generic executor for the encoding-reset commands
func resetEncoding(c *grumble.Context, encPtr *util.Encoder) error {
	if len(c.Args) != 1 {
		return fmt.Errorf("invalid number (%d) of arguments for `encoding %s`", len(c.Args), c.Command.Name)
	}

	encoding := c.Args[0]
	enc := util.NewEncoder(encoding)
	if enc == nil {
		return fmt.Errorf("uncognized encoding: %s", encoding)
	}
	*encPtr = enc
	c.App.Println(globalContext)
	return nil
}

func encodingCompleter(prefix string, args []string) []string {
	return filterStringWithPrefix([]string{
		"utf8",
		"int32",
		"int64",
		"bytes",
	}, prefix)
}
