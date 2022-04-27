/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cmd

import (
	"fmt"
	"strings"

	"github.com/desertbit/grumble"
	"github.com/apache/incubator-pegasus/pegic/executor/util"
	"github.com/apache/incubator-pegasus/pegic/interactive"
)

var supportedEncodings = []string{
	"utf8",
	"int32",
	"int64",
	"bytes",
	"javabytes",
	"asciihex",
}

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
		Help: fmt.Sprintf("Set encoding for hashkey. Supported encodings: %s", supportedEncodingsToString()),
		Run: func(c *grumble.Context) error {
			return resetEncoding(c, &globalContext.HashKeyEnc)
		},
		Args:      registerArgs,
		Completer: encodingCompleter,
	})

	rootCmd.AddCommand(&grumble.Command{
		Name: "sortkey",
		Help: fmt.Sprintf("Set encoding for sortkey. Supported encodings: %s", supportedEncodingsToString()),
		Run: func(c *grumble.Context) error {
			return resetEncoding(c, &globalContext.SortKeyEnc)
		},
		Args:      registerArgs,
		Completer: encodingCompleter,
	})

	rootCmd.AddCommand(&grumble.Command{
		Name: "value",
		Help: fmt.Sprintf("Set encoding for value. Supported encodings: %s", supportedEncodingsToString()),
		Run: func(c *grumble.Context) error {
			return resetEncoding(c, &globalContext.ValueEnc)
		},
		Args:      registerArgs,
		Completer: encodingCompleter,
	})

	interactive.App.AddCommand(rootCmd)
}

// resetEncoding is the generic executor for the encoding-reset commands
func resetEncoding(c *grumble.Context, encPtr *util.Encoder) error {
	if len(c.Args) != 1 {
		return fmt.Errorf("invalid number (%d) of arguments for `encoding %s`", len(c.Args), c.Command.Name)
	}

	encoding := c.Args.String("ENCODING")
	enc := util.NewEncoder(encoding)
	if enc == nil {
		return fmt.Errorf("uncognized encoding: %s", encoding)
	}
	*encPtr = enc
	c.App.Println(globalContext)
	return nil
}

func encodingCompleter(prefix string, args []string) []string {
	return filterStringWithPrefix(supportedEncodings, prefix)
}

func supportedEncodingsToString() string {
	return strings.Join(supportedEncodings, ",")
}

func registerArgs(a *grumble.Args) {
	a.String("ENCODING",
		fmt.Sprintf("The encoding from user string to raw bytes. Supported encodings: %s", supportedEncodingsToString()),
		grumble.Default("utf8"))
}
