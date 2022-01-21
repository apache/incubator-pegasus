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
		Name: "manual-compaction",
		Help: "manual compaction related commands",
	}

	rootCmd.AddCommand(&grumble.Command{
		Name: "start",
		Help: "start manual compaction for a specific table",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			targetLevel := c.Flags.Int("targetLevel")
			maxRunningCount := c.Flags.Int("maxConcurrentRunningCount")
			bottommost := c.Flags.Bool("bottommostLevelCompaction")
			if targetLevel < -1 {
				return fmt.Errorf("targetLevel should be greater than -1")
			}
			if maxRunningCount < 0 {
				return fmt.Errorf("maxRunningCount should be greater than 0")
			}
			return executor.StartManualCompaction(pegasusClient, tableName, targetLevel, maxRunningCount, bottommost)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Int("l", "targetLevel", -1, "compacted files move level, default value is -1")
			f.Int("c", "maxConcurrentRunningCount", 0, "max concurrent running count, default value is 0, no limited")
			f.Bool("b", "bottommostLevelCompaction", false, "bottommost level files will be compacted or not, default value is false")
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name: "query",
		Help: "query manual compaction progress for a specific table",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			return executor.QueryManualCompaction(pegasusClient, tableName)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
		},
	})

	shell.AddCommand(rootCmd)
}
