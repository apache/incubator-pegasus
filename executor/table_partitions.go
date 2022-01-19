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
	"strings"

	"github.com/pegasus-kv/admin-cli/tabular"
)

type PartitionStruct struct {
	Pidx            int32  `json:"pidx"`
	PrimaryAddr     string `json:"primary"`
	SecondariesAddr string `json:"secondaries"`
}

func GetTablePartitions(client *Client, tableName string) (partitions []PartitionStruct, err error) {
	resp, err := client.Meta.QueryConfig(tableName)
	if err != nil {
		return partitions, err
	}
	for _, partition := range resp.Partitions {
		p := PartitionStruct{}
		p.Pidx = partition.Pid.PartitionIndex

		primary := client.Nodes.MustGetReplica(partition.Primary.GetAddress())
		p.PrimaryAddr = primary.CombinedAddr()

		var secondaries []string
		for _, sec := range partition.Secondaries {
			secNode := client.Nodes.MustGetReplica(sec.GetAddress())
			secondaries = append(secondaries, secNode.CombinedAddr())
		}
		p.SecondariesAddr = strings.Join(secondaries, ",")

		partitions = append(partitions, p)
	}

	return partitions, nil
}

func ShowPartitionCount(client *Client, tableName string) error {
	resp, err := client.Meta.QueryConfig(tableName)
	if err != nil {
		return err
	}

	nodes, err := getNodesMap(client)
	if err != nil {
		return err
	}
	nodes, err = fillNodesInfo(nodes, resp.Partitions)
	if err != nil {
		return err
	}

	fmt.Println("[PartitionCount]")
	printNodesInfo(client, nodes)

	return nil
}

// ShowTablePartitions is table-partitions command
func ShowTablePartitions(client *Client, tableName string) error {

	if err := ShowPartitionCount(client, tableName); err != nil {
		return err
	}

	partitions, err := GetTablePartitions(client, tableName)
	if err != nil {
		return err
	}

	var partitionsInf []interface{}
	for _, partition := range partitions {
		partitionsInf = append(partitionsInf, partition)
	}

	fmt.Println("[PartitionDistribution]")
	tabular.Print(client, partitionsInf)
	return nil
}

func GetTablePartition(client *Client, tableName string, partitionIndex int32) (*PartitionStruct, error) {
	partitions, err := GetTablePartitions(client, tableName)
	if err != nil {
		return nil, err
	}
	if partitionIndex >= int32(len(partitions)) {
		return nil, fmt.Errorf("only have %d partitions, but you ask for %d", len(partitions), partitionIndex)
	}
	return &partitions[partitionIndex], nil
}
