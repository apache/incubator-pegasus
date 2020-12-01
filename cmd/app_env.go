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
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
	"fmt"

	"github.com/desertbit/grumble"
)

// NOTE: some old-version servers may not support some of the keys.
var predefinedAppEnvKeys = []string{
	"rocksdb.usage_scenario",
	"replica.deny_client_write",
	"replica.write_throttling",
	"replica.write_throttling_by_size",
	"default_ttl",
	"manual_compact.disabled",
	"manual_compact.max_concurrent_running_count",
	"manual_compact.once.trigger_time",
	"manual_compact.once.target_level",
	"manual_compact.once.bottommost_level_compaction",
	"manual_compact.periodic.trigger_time",
	"manual_compact.periodic.target_level",
	"manual_compact.periodic.bottommost_level_compaction",
	"rocksdb.checkpoint.reserve_min_count",
	"rocksdb.checkpoint.reserve_time_seconds",
	"replica.slow_query_threshold",
}

func init() {
	rootCmd := &grumble.Command{
		Name: "table-env",
		Help: "table environments related commands",
		// TODO(wutao): print commonly-used app-envs
	}
	rootCmd.AddCommand(&grumble.Command{
		Name:    "list",
		Aliases: []string{"ls"},
		Help:    "list the table environment binding to the table",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			return executor.ListAppEnvs(pegasusClient, c.UseTable)
		}),
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "set",
		Help: "set an environment with key and value",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			if len(c.Args) != 2 {
				return fmt.Errorf("invalid number (%d) of arguments for `table-env set`", len(c.Args))
			}
			return executor.SetAppEnv(pegasusClient, c.UseTable, c.Args[0], c.Args[1])
		}),
		AllowArgs: true,
		Completer: func(prefix string, args []string) []string {
			/* fill with predefined table-envs */
			if len(args) == 0 {
				return filterStringWithPrefix(predefinedAppEnvKeys, prefix)
			}
			return []string{}
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:    "delete",
		Aliases: []string{"del"},
		Help:    "delete table environments with specified key or key prefix",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("invalid number (%d) of arguments for `table-env delete`", len(c.Args))
			}
			return executor.DelAppEnv(pegasusClient, c.UseTable, c.Args[0], c.Flags.Bool("prefix"))
		}),
		AllowArgs: true,
		Flags: func(f *grumble.Flags) {
			f.BoolL("prefix", false, "to delete with key prefix")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "clear",
		Help: "clear all table environments",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			return executor.ClearAppEnv(pegasusClient, c.UseTable)
		}),
	})
	shell.AddCommand(rootCmd)
}
