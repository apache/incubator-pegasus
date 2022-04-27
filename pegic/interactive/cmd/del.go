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
	"github.com/pegasus-kv/pegic/executor"
	"github.com/pegasus-kv/pegic/interactive"
)

func init() {
	interactive.App.AddCommand(&grumble.Command{
		Name:  "del",
		Help:  "delete a record",
		Usage: "del <HASHKEY> <SORTKEY>",
		Run: requireUseTable(printEndingContext(func(c *grumble.Context) error {
			if len(c.Args) != 2 {
				return fmt.Errorf("invalid number (%d) of arguments for `del`", len(c.Args))
			}
			return executor.Del(globalContext, c.Args.String("HASHKEY"), c.Args.String("SORTKEY"))
		})),
		Args: func(a *grumble.Args) {
			a.String("HASHKEY", "HashKey")
			a.String("SORTKEY", "SortKey")
		},
	})
}
