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
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/diskbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "disk-migrate",
		Help: "migrate replica between the two disks within a specified ReplicaServer",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("g", "gpid", "", "gpid, for example, '2.1'")
			f.String("n", "node", "", "target node, for example, 127.0.0.1:34801")
			f.String("f", "from", "", "origin disk tag, for example, ssd1")
			f.String("t", "to", "", "target disk tag, for example, ssd2")
		},
		Run: func(c *grumble.Context) error {
			return executor.DiskMigrate(
				pegasusClient,
				c.Flags.String("node"),
				c.Flags.String("gpid"),
				c.Flags.String("from"),
				c.Flags.String("to"))
		},
	})

	// TODO(jiashuo1) need generate migrate strategy(step) depends the disk-info result to run
	shell.AddCommand(&grumble.Command{
		Name: "disk-balance",
		Help: "auto-migrate replica to let the disks space balance within the given ReplicaServer",
		Flags: func(f *grumble.Flags) {
			f.String("n", "node", "", "target node, for example, 127.0.0.1:34801")
			f.Int64("s", "size", 10<<10, "allow migrate min replica size, default 10GB")
			f.Bool("a", "auto", false, "auto balance node until the the node is balanced")
			// todo(jiashuo1) disk cleaner need "pegasus server execute update disk status",
			// todo(jiashuo1) the interval is 10min by default, so now have to wait >= 10min to make sure clean garbage
			f.Int("i", "interval", 650, "wait disk clean garbage replica interval")
		},
		Run: func(c *grumble.Context) error {
			return diskbalancer.BalanceDiskCapacity(pegasusClient, c.Flags.String("node"), c.Flags.Int64("size"), c.Flags.Int("interval"), c.Flags.Bool("auto"))
		},
	})

}
