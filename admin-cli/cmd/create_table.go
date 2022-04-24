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
	"github.com/pegasus-kv/admin-cli/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/admin-cli/shell"
)

func init() {
	longHelp := `Create a Pegasus table.

Please pay attention to the partition number. It usually depends on the table's on-disk storage size.
To achieve an predictable performance, you should keep the average partition size within a acceptable
range.`

	shell.AddCommand(&grumble.Command{
		Name:     "create",
		Help:     "create a table",
		Usage:    "create <table> [-p|--partitions <NUM>] [-r|--replica <NUM>]",
		LongHelp: longHelp,
		Run: func(c *grumble.Context) error {
			partitionCount := c.Flags.Int("partitions")
			if partitionCount%2 != 0 {
				return fmt.Errorf("partitions number must be a multiply of 2")
			}
			return executor.CreateTable(pegasusClient, c.Args.String("table"), partitionCount, c.Flags.Int("replica"))
		},
		Flags: func(f *grumble.Flags) {
			f.Int("p", "partitions", 4, "the number of partitions")
			f.Int("r", "replica", 3, "the number of replicas")
		},
		Args: func(a *grumble.Args) {
			a.String("table", "the table name")
		},
	})
}
