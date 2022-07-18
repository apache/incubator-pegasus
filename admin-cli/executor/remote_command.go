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

package executor

import (
	"fmt"

	"github.com/apache/incubator-pegasus/admin-cli/client"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/session"
	"github.com/olekukonko/tablewriter"
)

// RemoteCommand command.
func RemoteCommand(c *Client, nodeType session.NodeType, nodeAddr string, cmd string, args []string) error {
	var nodes []*util.PegasusNode
	if len(nodeAddr) == 0 {
		// send remote-commands to all nodeType nodes
		nodes = c.Nodes.GetAllNodes(nodeType)
	} else {
		n, err := c.Nodes.GetNode(nodeAddr, nodeType)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}

	results := client.BatchCallCmd(nodes, cmd, args)
	printCmdResults(c, cmd, args, results)
	return nil
}

func printCmdResults(client *Client, cmd string, args []string, results map[*util.PegasusNode]*client.CmdResult) {
	fmt.Fprintf(client, "CMD: %s %s\n\n", cmd, args)

	for n, res := range results {
		// print title for the node
		tb := tablewriter.NewWriter(client)
		tb.SetHeader([]string{fmt.Sprint(n)})
		tb.SetBorder(false)
		tb.SetAutoFormatHeaders(false)
		tb.Render()

		fmt.Fprintf(client, "%s\n", res)
		fmt.Fprintln(client)
	}
}
