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
	"github.com/desertbit/grumble"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "bulk-load",
		Help: "bulk load related commands",
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:  "start",
		Help:  "start bulk load for a specific table",
		Usage: `start <-a|--tableName TABLE_NAME> <-c|--clusterName CLUSTER_NAME> <-p|--providerType PROVIDER_TYPE> <-r|--rootPath ROOT_PATH>`,
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			if c.Flags.String("clusterName") == "" {
				return fmt.Errorf("clusterName cannot be empty")
			}
			if c.Flags.String("providerType") == "" {
				return fmt.Errorf("providerType cannot be empty")
			}
			if c.Flags.String("rootPath") == "" {
				return fmt.Errorf("rootPath cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			clusterName := c.Flags.String("clusterName")
			providerType := c.Flags.String("providerType")
			rootPath := c.Flags.String("rootPath")
			return executor.StartBulkLoad(pegasusClient, tableName, clusterName, providerType, rootPath)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.String("c", "clusterName", "", "cluster name")
			f.String("p", "providerType", "", "remote provider type")
			f.String("r", "rootPath", "", "remote root path")
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "query",
		Help:  "query bulk load status for a specific table or a specific partition",
		Usage: `query <-a|--tableName TABLE_NAME> [-i|--partitionIndex PARTITION_INDEX] [-d|--detailed SHOW_DETAILED_BULK_LOAD_STATUS]`,
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			partitionIndex := c.Flags.Int("partitionIndex")
			detailed := c.Flags.Bool("detailed")
			return executor.QueryBulkLoad(pegasusClient, tableName, partitionIndex, detailed)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Int("i", "partitionIndex", -1, "partition index, default value is -1, meaning show all partitions status")
			f.Bool("d", "detailed", false, "show detailed bulk load status, default value is false")
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "pause",
		Help:  "pause bulk load for a specific table",
		Usage: "pause <-a|--tableName TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			return executor.PauseBulkLoad(pegasusClient, tableName)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "restart",
		Help:  "restart bulk load for a specific table",
		Usage: "restart <-a|--tableName TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			return executor.RestartBulkLoad(pegasusClient, tableName)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "cancel",
		Help:  "cancel bulk load for a specific table",
		Usage: "cancel <-a|--tableName TABLE_NAME> [-f|--forced FORCED]",
		Run: func(c *grumble.Context) error {
			if c.Flags.String("tableName") == "" {
				return fmt.Errorf("tableName cannot be empty")
			}
			tableName := c.Flags.String("tableName")
			forced := c.Flags.Bool("forced")
			return executor.CancelBulkLoad(pegasusClient, tableName, forced)
		},
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("a", "tableName", "", "table name")
			f.Bool("f", "forced", false, "force cancel bulk load")
		},
	})

	shell.AddCommand(rootCmd)
}
