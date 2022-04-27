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
	rootCmd := &grumble.Command{
		Name:  "partition-index",
		Help:  "ADVANCED: calculate the partition index where the hashkey is routed to",
		Usage: "partition-index <HASHKEY>",
		Run: requireUseTable(func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("invalid number (%d) of arguments for `partition-index`", len(c.Args))
			}
			err := executor.PartitionIndex(globalContext, c.Args.String("HASHKEY"))
			if err != nil {
				return err
			}
			c.App.Println(globalContext)
			return nil
		}),
		Args: func(a *grumble.Args) {
			a.String("HASHKEY", "HashKey")
		},
	}
	interactive.App.AddCommand(rootCmd)
}
