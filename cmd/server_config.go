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
	"strconv"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

func init() {
	rootCmd := &grumble.Command{
		Name: "server-config",
		Help: "send http get/post to query/update config",
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:  "meta",
		Help:  "send config command to meta server",
		Flags: commandFlagFunc,
		Run: func(c *grumble.Context) error {
			return executeCommand(c, session.NodeTypeMeta)
		},
		Args: commandArgsFunc,
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "replica",
		Help:  "send config command to replica server",
		Flags: commandFlagFunc,
		Run: func(c *grumble.Context) error {
			return executeCommand(c, session.NodeTypeReplica)
		},
		Args: commandArgsFunc,
	})

	shell.AddCommand(rootCmd)
}

func commandFlagFunc(f *grumble.Flags) {
	/*define the flags*/
	f.String("n", "node", "", "specify server node address, such as 127.0.0.1:34801, empty mean all node")
}

func commandArgsFunc(a *grumble.Args) {
	a.StringList("command", "<CMD> [ARG1 ARG2 ...]", grumble.Default("[list]"))
}

func executeCommand(c *grumble.Context, ntype session.NodeType) error {
	defer func() {
		if err := recover(); err != nil {
			println(errMsg().Error())
		}
	}()

	cmd := c.Args.StringList("command")
	if len(cmd) == 0 {
		return errMsg()
	}
	if len(cmd) == 1 && cmd[0] == "list" {
		return executor.ConfigCommand(pegasusClient, ntype, c.Flags.String("node"), "", "list", 0)
	}
	if len(cmd) == 2 && cmd[1] == "get" {
		return executor.ConfigCommand(pegasusClient, ntype, c.Flags.String("node"), cmd[0], "get", 0)
	}
	if len(cmd) == 3 && cmd[1] == "set" {
		valueInt, err := strconv.ParseInt(cmd[2], 10, 64)
		if err != nil {
			return err
		}
		return executor.ConfigCommand(pegasusClient, ntype, c.Flags.String("node"), cmd[0], "set", valueInt)
	}

	return errMsg()
}

func errMsg() error {
	return fmt.Errorf("invalid command: \n\tconfig-commad meta/replica list: query all config\n\tconfig-commad" +
		" meta/replica {configName} `get` or `set {value}`: get or set config value")
}
