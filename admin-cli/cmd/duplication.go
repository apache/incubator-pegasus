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

	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/apache/incubator-pegasus/go-client/idl/admin"
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
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			return executor.QueryDuplication(pegasusClient, c.UseTable)
		}),
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "add",
		Help: "add a duplications to the table",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			if c.Flags.String("cluster") == "" {
				return fmt.Errorf("cluster cannot be empty")
			}
			return executor.AddDuplication(pegasusClient, c.UseTable, c.Flags.String("cluster"), c.Flags.Bool("sst"))
		}),
		Flags: func(f *grumble.Flags) {
			f.String("c", "cluster", "", "the destination where the source data is duplicated")
			f.Bool("s", "sst", true, "whether to duplicate checkpoint when duplication created")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:    "remove",
		Aliases: []string{"rm"},
		Help:    "remove a duplication from the table",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			if c.Flags.Int("dupid") == -1 {
				return fmt.Errorf("dupid cannot be empty")
			}
			return executor.ModifyDuplication(pegasusClient, c.UseTable, c.Flags.Int("dupid"), admin.DuplicationStatus_DS_REMOVED)
		}),
		Flags: func(f *grumble.Flags) {
			f.Int("d", "dupid", -1, "the dupid")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "pause",
		Help: "pause a duplication, it only support pause from `DS_LOG`",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			if c.Flags.Int("dupid") == -1 {
				return fmt.Errorf("dupid cannot be empty")
			}
			return executor.ModifyDuplication(pegasusClient, c.UseTable, c.Flags.Int("dupid"), admin.DuplicationStatus_DS_PAUSE)
		}),
		Flags: func(f *grumble.Flags) {
			f.Int("d", "dupid", -1, "the dupid")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name: "start",
		Help: "start a duplication, it only support start from `DS_PAUSE`",
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			if c.Flags.Int("dupid") == -1 {
				return fmt.Errorf("dupid cannot be empty")
			}
			return executor.ModifyDuplication(pegasusClient, c.UseTable, c.Flags.Int("dupid"), admin.DuplicationStatus_DS_LOG)
		}),
		Flags: func(f *grumble.Flags) {
			f.Int("d", "dupid", -1, "the dupid")
		},
	})
	shell.AddCommand(rootCmd)
}
