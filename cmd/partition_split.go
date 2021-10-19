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

func init() {
	rootCmd := &grumble.Command{
		Name: "partition-split",
		Help: "partition split related commands",
	}
	rootCmd.AddCommand(&grumble.Command{
		Name:  "start",
		Help:  "start partition split for a specific table",
		Usage: `start <-a|--tableName TABLE_NAME> <-p|--newPartitionCount NEW_PARTITION_COUNT>`,
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			if c.Flags.Int("newPartitionCount") == 0 {
				return fmt.Errorf("newPartitionCount cannot be empty")
			}

			tableName := c.Flags.String("tableName")
			newPartitionCount := c.Flags.Int("newPartitionCount")
			return executor.StartPartitionSplit(pegasusClient, tableName, newPartitionCount)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Int("p", "newPartitionCount", 0, "new_partition_count, should be double of current partition_count")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:  "query",
		Help:  "query partition split status for a specific table",
		Usage: "query <-a|--tableName TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			return executor.QuerySplitStatus(pegasusClient, tableName)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:  "pause",
		Help:  "pause partition split for specific partition or all partitions of a table",
		Usage: "pause <-a|--tableName TABLE_NAME> [-i|--parentPidx PARENT_PIDX]",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			parentPidx := c.Flags.Int("parentPidx")
			return executor.PausePartitionSplit(pegasusClient, tableName, parentPidx)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Int("i", "parentPidx", -1, "parent partition index")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:  "restart",
		Help:  "restart partition split for specific partition or all partitions of a table",
		Usage: "restart <-a|--tableName TABLE_NAME> [-i|--parentPidx PARENT_PIDX]",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			parentPidx := c.Flags.Int("parentPidx")
			return executor.RestartPartitionSplit(pegasusClient, tableName, parentPidx)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Int("i", "parentPidx", -1, "parent partition index")
		},
	})
	rootCmd.AddCommand(&grumble.Command{
		Name:  "cancel",
		Help:  "cancel partition split for a specific table",
		Usage: "cancel <-a|--tableName TABLE_NAME> <-p|--oldPartitionCount OLD_PARTITION_COUNT>",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			if c.Flags.Int("oldPartitionCount") == 0 {
				return fmt.Errorf("oldPartitionCount cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			oldPartitionCount := c.Flags.Int("oldPartitionCount")
			return executor.CancelPartitionSplit(pegasusClient, tableName, oldPartitionCount)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Int("p", "oldPartitionCount", 0, "table partition count before split")
		},
	})
	shell.AddCommand(rootCmd)
}
