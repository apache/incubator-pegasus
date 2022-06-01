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
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits/nodesbalancer"
	"github.com/apache/incubator-pegasus/admin-cli/shell"
	"github.com/desertbit/grumble"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "nodes-balancer",
		Help: "migrate replica among the replica server to balance the capacity of cluster, please " +
			"make sure the server config is right, detail see https://github.com/apache/incubator-pegasus/pull/969",
		Flags: func(a *grumble.Flags) {
			a.BoolL("auto", false, "whether to migrate replica until all nodes is balanced, false "+
				"by default, which means it just migrate one replica")
		},
		Run: func(c *grumble.Context) error {
			auto := c.Flags.Bool("auto")
			return nodesbalancer.BalanceNodeCapacity(pegasusClient, auto)
		},
	})
}
