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
	"strings"

	"github.com/apache/incubator-pegasus/admin-cli/client"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/collector/aggregate"
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
// When listing nodes fails, willExit == true means call os.Exit().
func NewClient(writer io.Writer, metaAddrs []string, willExit bool) (*Client, error) {
	meta := client.NewRPCBasedMeta(metaAddrs)

	// TODO(wutao): initialize replica-nodes lazily
	nodes, err := meta.ListNodes()
	if err != nil {
		fmt.Printf("Error: failed to list nodes [%s]\n", err)
		if willExit {
			os.Exit(1)
		}
		return nil, fmt.Errorf("failed to list nodes [%s]", err)
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
	}, nil
}

func CloseClient(client *Client) error {
	var errorStrings []string
	err := client.Meta.Close()
	if err != nil {
		fmt.Printf("Error: failed to close meta session [%s].\n", err)
		errorStrings = append(errorStrings, err.Error())
	}

	client.Perf.Close()

	err = client.Nodes.CloseAllNodes()
	if err != nil {
		fmt.Printf("Error: failed to close nodes session [%s].\n", err)
		errorStrings = append(errorStrings, err.Error())
	}
	if len(errorStrings) != 0 {
		return fmt.Errorf("%s", strings.Join(errorStrings, "\n"))
	}
	return nil
}
