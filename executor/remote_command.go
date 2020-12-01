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
	"admin-cli/executor/util"
	"context"
	"fmt"
	"sync"
	"time"

	adminCli "github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/olekukonko/tablewriter"
)

// RemoteCommand command.
func RemoteCommand(client *Client, nodeType session.NodeType, nodeAddr string, cmd string, args []string) error {
	rc := &adminCli.RemoteCommand{
		Command:   cmd,
		Arguments: args,
	}

	var nodes []*util.PegasusNode
	if len(nodeAddr) == 0 {
		// send remote-commands to all nodeType nodes
		nodes = client.Nodes.GetAllNodes(nodeType)
	} else {
		n, err := client.Nodes.GetNode(nodeAddr, nodeType)
		if err != nil {
			return err
		}
		nodes = append(nodes, n)
	}

	results := batchCallCmd(nodes, rc)
	printCmdResults(client, rc, results)
	return nil
}

type cmdResult struct {
	resp string
	err  error
}

func (c *cmdResult) String() string {
	if c.err != nil {
		return fmt.Sprintf("failure: %s", c.err)
	}
	return c.resp
}

func batchCallCmd(nodes []*util.PegasusNode, cmd *adminCli.RemoteCommand) map[*util.PegasusNode]*cmdResult {
	results := make(map[*util.PegasusNode]*cmdResult)
	for _, n := range nodes {
		results[n] = nil
	}

	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *util.PegasusNode) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			result, err := cmd.Call(ctx, node.Session())
			if err != nil {
				results[node] = &cmdResult{err: err}
			} else {
				results[node] = &cmdResult{resp: result}
			}
			wg.Done()
		}(n)
	}
	wg.Wait()

	return results
}

func printCmdResults(client *Client, cmd *adminCli.RemoteCommand, results map[*util.PegasusNode]*cmdResult) {
	fmt.Fprintf(client, "CMD: %s %s\n\n", cmd.Command, cmd.Arguments)

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
