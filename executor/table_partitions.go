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
	"admin-cli/tabular"
	"context"
	"strings"
	"time"
)

// ShowTablePartitions is table-partitions command
func ShowTablePartitions(client *Client, tableName string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	resp, err := client.Meta.QueryConfig(ctx, tableName)
	if err != nil {
		return err
	}

	type partitionStruct struct {
		Pidx            int32  `json:"pidx"`
		PrimaryAddr     string `json:"primary"`
		SecondariesAddr string `json:"secondaries"`
	}

	var partitions []interface{}
	for _, partition := range resp.Partitions {
		p := partitionStruct{}
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

	tabular.Print(client, partitions)
	return nil
}
