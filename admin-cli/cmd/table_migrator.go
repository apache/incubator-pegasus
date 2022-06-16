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
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/tablemigrator"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "table-migrator",
		Help: "migrate table from current cluster to another via table duplication and metaproxy",
		Flags: func(f *grumble.Flags) {
			f.String("t", "table", "", "table name")
			f.String("n", "node", "", "zk node, addrs:port, default equal with peagsus "+
				"cluster zk addrs, you can use `cluster-info` to show it")
			f.String("r", "root", "", "zk root path. the tool will update table addrs in "+
				"the path of meatproxy, if you don't specify it, that is means user need manual-switch the table addrs")
			f.String("c", "cluster", "", "target cluster name")
			f.String("m", "meta", "", "target meta list")
			f.Float64("p", "threshold", 100000, "pending mutation throshold when server will reject all write request")
		},
		Run: func(c *grumble.Context) error {
			return tablemigrator.MigrateTable(pegasusClient, c.Flags.String("table"),
				c.Flags.String("node"), c.Flags.String("root"),
				c.Flags.String("cluster"), c.Flags.String("meta"), c.Flags.Float64("threshold"))
		},
	})
}
