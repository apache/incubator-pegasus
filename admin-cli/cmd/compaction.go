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
	shell.AddCommand(&grumble.Command{
		Name: "add-compaction-operation",
		Help: "add compaction operation and the corresponding rules",
		Flags: func(f *grumble.Flags) {
			/**
			 *	operations
			 **/
			f.String("o", "operation-type", "", "operation type, for example: delete/update-ttl")
			// update ttl operation
			f.String("u", "ttl-type", "", "update ttl operation type, for example: from-now/from-current/timestamp")
			f.Uint("v", "time-value", 0, "time value")
			/**
			 *  rules
			 **/
			// hashkey filter
			f.StringL("hashkey-pattern", "", "hash key pattern")
			f.StringL("hashkey-match", "anywhere", "hash key's match type, for example: anywhere/prefix/postfix")
			// sortkey filter
			f.StringL("sortkey-pattern", "", "sort key pattern")
			f.StringL("sortkey-match", "anywhere", "sort key's match type, for example: anywhere/prefix/postfix")
			// ttl filter
			f.Int64L("start-ttl", -1, "ttl filter, start ttl")
			f.Int64L("stop-ttl", -1, "ttl filter, stop ttl")
		},
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			var params = &executor.CompactionParams{
				OperationType:  c.Flags.String("operation-type"),
				UpdateTTLType:  c.Flags.String("ttl-type"),
				TimeValue:      c.Flags.Uint("time-value"),
				HashkeyPattern: c.Flags.String("hashkey-pattern"),
				HashkeyMatch:   c.Flags.String("hashkey-match"),
				SortkeyPattern: c.Flags.String("sortkey-pattern"),
				SortkeyMatch:   c.Flags.String("sortkey-match"),
				StartTTL:       c.Flags.Int64("start-ttl"),
				StopTTL:        c.Flags.Int64("stop-ttl"),
			}
			return executor.SetCompaction(
				pegasusClient,
				c.UseTable,
				params)
		}),
	})
}
