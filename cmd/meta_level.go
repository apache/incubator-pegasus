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

	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

var predefinedMetaLevel = []string{
	"lively",
	"steady",
}

func init() {
	rootCmd := &grumble.Command{
		Name: "meta-level",
		Help: "Get the current meta function level",
		Run: func(c *grumble.Context) error {
			return executor.GetMetaLevel(pegasusClient)
		},
	}

	longHelp := `Reset the rebalancing level of the cluster.

The possible rebalancing levels include "lively", "steady", "blind", "freezed", "stopped".
The default level of cluster is "steady", which indicates that no automatic rebalancing will be performed.
Setting to "lively" can enable auto-rebalancing.

Documentation:
  https://pegasus.apache.org/administration/rebalance`

	rootCmd.AddCommand(
		&grumble.Command{
			Name:     "set",
			Help:     "reset the meta function level",
			LongHelp: longHelp,
			Usage:    "meta-level set <LEVEL>",
			Run: func(c *grumble.Context) error {
				if len(c.Args) != 1 {
					return fmt.Errorf("invalid number (%d) of arguments for `meta-level set`", len(c.Args))
				}
				return executor.SetMetaLevel(pegasusClient, c.Args[0])
			},
			Completer: func(prefix string, args []string) []string {
				if len(args) == 0 {
					return filterStringWithPrefix(predefinedMetaLevel, prefix)
				}
				return []string{}
			},
			AllowArgs: true,
		},
	)
	shell.App.AddCommand(rootCmd)
}
