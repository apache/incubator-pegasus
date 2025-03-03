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

package util

import (
	"strings"

	"github.com/apache/incubator-pegasus/collector/aggregate"
)

func GetPartitionStat(perfSession *aggregate.PerfSession, counter string) map[string]float64 {
	// gpid->value
	partitionStats := make(map[string]float64)
	stats, err := perfSession.GetPerfCounters(counter)
	if err != nil {
		panic(err)
	}

	for _, stat := range stats {
		ret := strings.Split(stat.Name, "@")
		partitionStats[ret[1]] = stat.Value
	}
	return partitionStats
}

// GetNodeStats returns a mapping of [node address => node stats]
func GetNodeStats(perfClient *aggregate.PerfClient) (map[string]*aggregate.NodeStat, error) {
	var nodesStats = make(map[string]*aggregate.NodeStat)

	nodes, err := perfClient.GetNodeStats("replica")
	if err != nil {
		return nil, err
	}
	for _, node := range nodes {
		nodesStats[node.Addr] = &aggregate.NodeStat{
			Addr:  node.Addr,
			Stats: make(map[string]float64),
		}
	}
	for _, node := range nodes {
		for name, value := range node.Stats {
			name = getCounterName(name)
			nodesStats[node.Addr].Stats[name] += value
		}
	}
	return nodesStats, nil
}

func getCounterName(name string) string {
	ret := strings.Split(name, "@")
	if len(ret) != 0 {
		return ret[0]
	}
	return name
}
