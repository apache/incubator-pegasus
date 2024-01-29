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

	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/desertbit/grumble"
)

// TODO(jiashuo1) The command will be replaced by `server-config` in server_config.go
func init() {
	rootCmd := &grumble.Command{
		Name: "remote-command",
		Help: "send remote command, for example, remote-command meta or replica",
	}

	rootCmd.AddCommand(&grumble.Command{
		Name:  "meta",
		Help:  "send remote command to meta server",
		Flags: remoteCommandFlagFunc,
		Run: func(c *grumble.Context) error {
			return executeRemoteCommand(c, session.NodeTypeMeta)
		},
		Args: func(a *grumble.Args) {
			a.StringList("command", "<CMD> [ARG1 ARG2 ...]", grumble.Default("help"))
		},
	})

	rootCmd.AddCommand(&grumble.Command{
		Name:  "replica",
		Help:  "send remote command to replica server",
		Flags: remoteCommandFlagFunc,
		Run: func(c *grumble.Context) error {
			return executeRemoteCommand(c, session.NodeTypeReplica)
		},
		Args: func(a *grumble.Args) {
			a.StringList("command", "<CMD> [ARG1 ARG2 ...]", grumble.Default("help"))
		},
	})

	shell.AddCommand(rootCmd)
}

func remoteCommandFlagFunc(f *grumble.Flags) {
	/*define the flags*/
	f.String("n", "node", "", "specify server node address, such as 127.0.0.1:34801, empty mean all node")
}

func executeRemoteCommand(c *grumble.Context, ntype session.NodeType) error {
	command := c.Args.StringList("command")
	return executor.RemoteCommand(pegasusClient, ntype, c.Flags.String("node"), command[0], command[1:])
}
