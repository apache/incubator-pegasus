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
	"sync"

	"github.com/apache/incubator-pegasus/admin-cli/tabular"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/olekukonko/tablewriter"
	batchErr "k8s.io/apimachinery/pkg/util/errors"
)

type nodeInfoStruct struct {
	Address           string `json:"Node"`
	Status            string `json:"Status"`
	ReplicaTotalCount int    `json:"Replica"`
	PrimaryCount      int    `json:"Primary"`
	SecondaryCount    int    `json:"Secondary"`
}

// ListNodes is nodes command.
func ListNodes(client *Client) error {
	nodes, err := getNodesMap(client)
	if err != nil {
		return err
	}

	apps, errTable := client.Meta.ListAvailableApps()
	if errTable != nil {
		return errTable
	}

	var mu sync.Mutex
	var funcs []func() error

	for _, info := range apps {
		info := info
		funcs = append(funcs, func() error {
			queryCfgResp, err := client.Meta.QueryConfig(info.AppName)
			if err != nil {
				return fmt.Errorf("query config failed for \"%s\" : %s", info.AppName, err)
			}
			mu.Lock()
			nodes, err = fillNodesInfo(nodes, queryCfgResp.Partitions)
			mu.Unlock()
			if err != nil {
				return fmt.Errorf("fill nodes replica count info failed: %s", err)
			}
			return nil
		})
	}

	err = batchErr.AggregateGoroutines(funcs...)
	if err != nil {
		return err
	}

	printNodesInfo(client, nodes)
	return nil
}

func getNodesMap(client *Client) (map[string]*nodeInfoStruct, error) {
	nodeInfos, err := client.Meta.ListNodes()
	if err != nil {
		return nil, err
	}
	nodes := make(map[string]*nodeInfoStruct)
	for _, ninfo := range nodeInfos {
		n := client.Nodes.MustGetReplica(ninfo.Address.GetAddress())
		nodes[ninfo.Address.GetAddress()] = &nodeInfoStruct{
			Address: n.CombinedAddr(),
			Status:  ninfo.Status.String(),
		}
	}
	return nodes, nil
}

func fillNodesInfo(nodes map[string]*nodeInfoStruct, partitions []*replication.PartitionConfiguration) (map[string]*nodeInfoStruct, error) {
	for _, part := range partitions {
		n := nodes[part.Primary.GetAddress()]
		if part.Primary.GetRawAddress() != 0 {
			if n != nil {
				n.PrimaryCount++
				n.ReplicaTotalCount++
			} else {
				return nil, fmt.Errorf("inconsistent state: nodes are updated")
			}
		}

		for _, sec := range part.Secondaries {
			n := nodes[sec.GetAddress()]
			n.SecondaryCount++
			n.ReplicaTotalCount++
		}
	}
	return nodes, nil
}

func printNodesInfo(client *Client, nodes map[string]*nodeInfoStruct) {
	// render in tabular form
	var nodeList []interface{}
	for _, n := range nodes {
		nodeList = append(nodeList, *n)
	}
	util.SortStructsByField(nodeList, "Address")
	tabular.New(client, nodeList, func(t *tablewriter.Table) {
		footerWithTotalCount(t, nodeList)
	}).Render()
}

func footerWithTotalCount(tbWriter *tablewriter.Table, nlist []interface{}) {
	var aliveCnt, unaliveCnt int
	var totalRepCnt, totalPriCnt, totalSecCnt int
	for _, element := range nlist {
		n := element.(nodeInfoStruct)
		totalRepCnt += n.ReplicaTotalCount
		totalPriCnt += n.PrimaryCount
		totalSecCnt += n.SecondaryCount
		if n.Status == admin.NodeStatus_NS_ALIVE.String() {
			aliveCnt++
		} else {
			unaliveCnt++
		}
	}
	tbWriter.SetFooter([]string{
		fmt.Sprintf("Alive(%d) | Unalive(%d)", aliveCnt, unaliveCnt),
		fmt.Sprintf("Total(%d)", len(nlist)),
		fmt.Sprintf("%d", totalRepCnt),
		fmt.Sprintf("%d", totalPriCnt),
		fmt.Sprintf("%d", totalSecCnt),
	})
}
