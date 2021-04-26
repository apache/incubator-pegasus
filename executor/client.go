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
	"io"
	"os"

	"github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/executor/util"
	"github.com/pegasus-kv/collector/aggregate"
)

// Client represents as a manager of various SDKs that
// can access both Pegasus ReplicaServer and MetaServer.
type Client struct {
	// Every command should use Client as the fmt.Fprint's writer.
	io.Writer

	// to access administration APIs
	Meta client.Meta

	// to obtain perf-counters of ReplicaServers
	Perf *aggregate.PerfClient

	Nodes *util.PegasusNodeManager
}

// NewClient creates a client for accessing Pegasus cluster for use of admin-cli.
func NewClient(writer io.Writer, metaAddrs []string) *Client {
	meta := client.NewRPCBasedMeta(metaAddrs)

	// TODO(wutao): initialize replica-nodes lazily
	nodes, err := meta.ListNodes()
	if err != nil {
		fmt.Fprintf(writer, "fatal: failed to list nodes [%s]\n", err)
		os.Exit(1)
	}

	var replicaAddrs []string
	for _, node := range nodes {
		replicaAddrs = append(replicaAddrs, node.Address.GetAddress())
	}

	return &Client{
		Writer: writer,
		Meta:   meta,
		Nodes:  util.NewPegasusNodeManager(metaAddrs, replicaAddrs),
		Perf:   aggregate.NewPerfClient(metaAddrs),
	}
}
