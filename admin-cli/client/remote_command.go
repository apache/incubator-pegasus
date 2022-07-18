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

package client

import (
	"context"
	"fmt"
	"sync"

	"github.com/apache/incubator-pegasus/admin-cli/util"
	adminCli "github.com/apache/incubator-pegasus/go-client/admin"
)

// CmdResult is the result of remote command to a node.
type CmdResult struct {
	resp string
	err  error
}

func (c *CmdResult) String() string {
	if c.err != nil {
		return fmt.Sprintf("failure: %s", c.err)
	}
	return c.resp
}

func (c *CmdResult) Failed() bool {
	return c.err != nil
}

func (c *CmdResult) Error() error {
	return c.err
}

func (c *CmdResult) RespBody() string {
	return c.resp
}

// BatchCallCmd performs remote commands in parallel to multiple nodes.
func BatchCallCmd(nodes []*util.PegasusNode, cmd string, args []string) map[*util.PegasusNode]*CmdResult {
	results := make(map[*util.PegasusNode]*CmdResult)

	rc := &adminCli.RemoteCommand{
		Command:   cmd,
		Arguments: args,
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *util.PegasusNode) {
			ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
			defer cancel()
			result, err := rc.Call(ctx, node.Session())
			mu.Lock()
			if err != nil {
				results[node] = &CmdResult{err: err}
			} else {
				results[node] = &CmdResult{resp: result}
			}
			mu.Unlock()
			wg.Done()
		}(n)
	}
	wg.Wait()

	return results
}

// CallCmd calls remote command to a single node.
func CallCmd(n *util.PegasusNode, cmd string, args []string) *CmdResult {
	rc := &adminCli.RemoteCommand{
		Command:   cmd,
		Arguments: args,
	}

	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	result, err := rc.Call(ctx, n.Session())
	if err != nil {
		return &CmdResult{err: err}
	}
	return &CmdResult{resp: result}
}
