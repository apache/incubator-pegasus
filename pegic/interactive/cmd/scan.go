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

	"github.com/apache/incubator-pegasus/pegic/executor"
	"github.com/apache/incubator-pegasus/pegic/interactive"
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
		Run: requireUseTable(printEndingContext(func(c *grumble.Context) error {
			cmd, err := newScanCommand(c)
			if err != nil {
				return err
			}
			return cmd.IterateAll(globalContext)
		})),
		Flags: scanFlags,
		Args: func(a *grumble.Args) {
			a.String("HASHKEY", "HashKey")
		},
	}

	scanCmd.AddCommand(&grumble.Command{
		Name:  "count",
		Help:  "scan records under the hashkey",
		Usage: "\nscan " + usage,
		Run: requireUseTable(printEndingContext(func(c *grumble.Context) error {
			cmd, err := newScanCommand(c)
			if err != nil {
				return err
			}
			cmd.CountOnly = true
			return cmd.IterateAll(globalContext)
		})),
		Flags: scanFlags,
		Args: func(a *grumble.Args) {
			a.String("HASHKEY", "HashKey")
		},
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

	cmd := &executor.ScanCommand{HashKey: c.Args.String("HASHKEY")}
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
