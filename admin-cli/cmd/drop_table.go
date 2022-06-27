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
	longHelp := `Drop a table.

After table dropped, it's inaccessible to clients.

To prevent misoperation, the actual table data will not be immediately deleted after calling this command.
Instead, it will be reserved for a period on disk. This feature we call it "soft-deletion".
The soft-deletion period can be specified with '-r' flag.
Please BE CAREFUL not to set a too short RESERVE_SECONDS on valuable data!!!

Sample:
  drop test_table -r 86400
This command will reserve the data of "test_table" for 1 day (86400 seconds).

Documentation:
  https://pegasus.apache.org/administration/table-soft-delete`

	shell.AddCommand(&grumble.Command{
		Name:     "drop",
		Help:     "drop a table",
		LongHelp: longHelp,
		Usage:    "drop [-r|--reserved <RESERVE_SECONDS>] <TABLE>",

		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("must specify a table name")
			}
			return executor.DropTable(pegasusClient, c.Args.String("table"), c.Flags.Int64("reserved"))
		},
		Flags: func(f *grumble.Flags) {
			// 7 days by default.
			f.Int64("r", "reserved", 86400*7, "the soft-deletion period, which is the time before table actually deleted")
		},
		Args: func(a *grumble.Args) {
			a.String("table", "the table name")
		},
	})
}
