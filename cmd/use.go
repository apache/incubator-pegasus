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
	"admin-cli/shell"
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/desertbit/grumble"
)

var useTable string

var cachedTableNames []string

// whether table names are previously cached
var tablesCached bool = false

func init() {
	shell.AddCommand(&grumble.Command{
		Name:  "use",
		Help:  "select a table",
		Usage: "use <TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				c.App.Println(c.Command.Usage)
				return fmt.Errorf("invalid number (%d) of arguments for `use`", len(c.Args))
			}
			useTable = c.Args[0]
			// TODO(wutao): verify if the use table exists
			c.App.Println("ok")
			return nil
		},
		AllowArgs: true,
		Completer: func(prefix string, args []string) []string {
			return useCompletion(prefix)
		},
	})
}

func useCompletion(prefix string) []string {
	if tablesCached {
		// returns all tables
		return cachedTableNames
	}

	// TODO(wutao): auto-completes in background

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	resp, err := pegasusClient.Meta.ListApps(ctx, &admin.ListAppsRequest{
		Status: admin.AppStatus_AS_AVAILABLE,
	})
	if err != nil {
		return []string{}
	}

	tableNames := []string{}
	for _, app := range resp.Infos {
		tableNames = append(tableNames, app.AppName)
	}
	cachedTableNames = tableNames
	tablesCached = true
	return tableNames
}
