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

package shell

import (
	"fmt"

	"github.com/desertbit/grumble"
)

// Context is the command execution context.
type Context struct {
	*grumble.Context

	// The used table.
	UseTable string
}

var globalUseTable string

// SetUseTable selects a table for the following commands.
func SetUseTable(tb string) {
	globalUseTable = tb
}

// RequireUseTable is a wrapper of grumble.Command.Run. The command will fail if no table was selected.
func RequireUseTable(run func(*Context) error) func(c *grumble.Context) error {
	grumbleRun := func(c *grumble.Context) error {
		if globalUseTable == "" {
			return fmt.Errorf("please USE a table first")
		}
		ctx := &Context{Context: c, UseTable: globalUseTable}
		return run(ctx)
	}
	return grumbleRun
}
