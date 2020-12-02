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
	"github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/olekukonko/tablewriter"
	"github.com/pegasus-kv/admin-cli/tabular"
)

// ServerInfo command
func ServerInfo(client *Client) error {

	nodes := client.Nodes.GetAllNodes(session.NodeTypeMeta)
	nodes = append(nodes, client.Nodes.GetAllNodes(session.NodeTypeReplica)...)

	results := batchCallCmd(nodes, &admin.RemoteCommand{
		Command:   "server-info",
		Arguments: []string{},
	})

	type serverInfoStruct struct {
		Server  string `json:"server"`
		Node    string `json:"node"`
		Version string `json:"version"`
	}
	// always print meta-server first.
	var rowList []interface{}
	for _, tp := range []session.NodeType{session.NodeTypeMeta, session.NodeTypeReplica} {
		for n, result := range results {
			if n.Type != tp {
				continue
			}
			rowList = append(rowList, serverInfoStruct{
				Server:  string(n.Type),
				Node:    n.CombinedAddr(),
				Version: result.String(),
			})
		}
	}

	tabular.New(client, rowList, func(table *tablewriter.Table) {
		table.SetAutoWrapText(false)
	}).Render()
	return nil
}
