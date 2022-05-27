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
	"strings"

	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/nodesmigrator"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "nodes-migrator",
		Help: "migrate all replicas from nodes to other node",
		Flags: func(a *grumble.Flags) {
			a.String("f", "from", "", "origin nodes list, such as: 127.0.0.1:34801,127.0.0.2:34801")
			a.String("t", "to", "", "target nodes list, such as: 127.0.0.3:34802,127.0.0.3:34802")
			a.String("a", "table", "", "specify the table list(default migrate all table)")
			a.Int("c", "concurrent", 1, "max concurrent replica migrate task count(default 1)")
			a.BoolL("batch_table", false, "whether to batch migrate multi tables(default false, tips: set true "+
				"when only table partition size is less 1GB)")
			a.BoolL("batch_target", false, "whether to batch migrate to multi targets(default false, "+
				"tips: disable it will decrease the influence but increase time)")
			a.IntL("final_target", 0, "specify the final target count, note: this count is final "+
				"count in cluster, and len(to) must be equal with it")
		},
		Run: func(c *grumble.Context) error {
			from := strings.Split(c.Flags.String("from"), ",")
			to := strings.Split(c.Flags.String("to"), ",")
			tables := strings.Split(c.Flags.String("table"), ",")
			concurrent := c.Flags.Int("concurrent")
			batchTable := c.Flags.Bool("batch_table")
			batchTarget := c.Flags.Bool("batch_target")
			finalTargets := c.Flags.Int("final_target")
			return nodesmigrator.MigrateAllReplicaToNodes(pegasusClient, from, to, tables, batchTable, batchTarget, finalTargets, concurrent)
		},
	})
}
