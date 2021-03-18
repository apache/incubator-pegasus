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
	"errors"
	"os"
	"pegic/executor"
	"strings"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/desertbit/grumble"
)

var globalContext *executor.Context

func Init(metaAddrs []string) error {
	// validate meta addresses
	_, err := session.ResolveMetaAddr(metaAddrs)
	if err != nil {
		return err
	}

	globalContext = executor.NewContext(os.Stdout, metaAddrs)
	return nil
}

func requireUseTable(run func(*grumble.Context) error) func(c *grumble.Context) error {
	grumbleRun := func(c *grumble.Context) error {
		if globalContext.UseTable == nil {
			c.App.PrintError(errors.New("please USE a table first"))
			c.App.Println("Usage: USE <TABLE_NAME>")
			return nil
		}
		return run(c)
	}
	return grumbleRun
}

// filterStringWithPrefix returns strings with the same prefix.
// This function is commonly used for the auto-completion of commands.
func filterStringWithPrefix(strs []string, prefix string) []string {
	var result []string
	for _, s := range strs {
		if strings.HasPrefix(s, prefix) {
			result = append(result, s)
		}
	}
	return result
}

func printEndingContext(run func(*grumble.Context) error) func(c *grumble.Context) error {
	grumbleRun := func(c *grumble.Context) error {
		err := run(c)
		c.App.Println(globalContext)
		return err
	}
	return grumbleRun
}
