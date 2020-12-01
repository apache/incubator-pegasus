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
	"admin-cli/executor"
	"admin-cli/shell"

	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "disk-capacity",
		Help: "query disk capacity info ",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("n", "node", "", "node address(ip:port), for example, 127.0.0.1:34801")
			f.String("d", "disk", "", "disk tag, for example, ssd1")
			f.String("t", "table", "", "table name, for example, temp")
		},
		Run: func(c *grumble.Context) error {
			return executor.QueryDiskInfo(
				pegasusClient,
				executor.CapacitySize,
				c.Flags.String("node"),
				c.Flags.String("table"),
				c.Flags.String("disk"))
		},
	})

	shell.AddCommand(&grumble.Command{
		Name: "disk-replica",
		Help: "query disk replica count info",
		Flags: func(f *grumble.Flags) {
			/*define the flags*/
			f.String("n", "node", "", "node address(ip:port), for example, 127.0.0.1:34801")
			f.String("t", "table", "", "table name, for example, temp")
		},
		Run: func(c *grumble.Context) error {
			return executor.QueryDiskInfo(
				pegasusClient,
				executor.ReplicaCount,
				c.Flags.String("node"),
				c.Flags.String("table"),
				"")
		},
	})
}
