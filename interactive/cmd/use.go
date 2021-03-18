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
	"context"
	"fmt"
	"pegic/interactive"
	"strings"
	"time"

	"github.com/desertbit/grumble"
)

func init() {
	interactive.App.AddCommand(&grumble.Command{
		Name:  "use",
		Help:  "select a table",
		Usage: "use <TABLE_NAME>",
		Run: func(c *grumble.Context) error {
			if len(c.Args) != 1 {
				return fmt.Errorf("invalid number (%d) of arguments for `use`", len(c.Args))
			}

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			var tableName = c.Args.String("TABLE")
			var err error
			globalContext.UseTable, err = globalContext.OpenTable(ctx, tableName)
			if err != nil && strings.Contains(err.Error(), "ERR_OBJECT_NOT_FOUND") {
				// give a simple prompt to this special error
				return fmt.Errorf("table \"%s\" does not exist", tableName)
			}
			if err != nil {
				return err
			}
			globalContext.UseTableName = tableName

			c.App.Println("ok")
			return nil
		},
		Args: func(a *grumble.Args) {
			a.String("TABLE", "The table name to access.")
		},
	})
}
