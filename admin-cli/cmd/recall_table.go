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
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	longHelp := `Recall a dropped table.

Before the table being physically deleted, you can recall it alive and restore its data.
This command requires the table ID to restore, which you can see via "ls --drop" command.
By default, recall will create a new table with the same name as before.
You can specify a new table name as the second argument.

Sample:
  recall 16 new_table

Documentation:
  https://pegasus.apache.org/administration/table-soft-delete`

	shell.AddCommand(&grumble.Command{
		Name:     "recall",
		Help:     "recall the dropped table",
		LongHelp: longHelp,
		Usage:    "recall <ORIGIN_TABLE_ID> [NEW_TABLE_NAME]",
		Args: func(a *grumble.Args) {
			a.Int("originTableID", "the orginal table ID")
			a.String("newTableName", "the name of the recreated table", grumble.Default(""))
		},
		Run: func(c *grumble.Context) error {
			originTableID := c.Args.Int("originTableID")
			newTableName := c.Args.String("newTableName")
			return executor.RecallTable(pegasusClient, originTableID, newTableName)
		},
	})
}
