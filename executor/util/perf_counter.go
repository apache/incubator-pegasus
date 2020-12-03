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
	"fmt"
	"strings"

	"github.com/pegasus-kv/collector/aggregate"
)

func GetPartitionStat(perfSession *aggregate.PerfSession, counter string, gpid string) int64 {
	counters, err := perfSession.GetPerfCounters(fmt.Sprintf("%s@%s", counter, gpid))
	if err != nil {
		panic(err)
	}

	if len(counters) != 1 {
		panic(fmt.Sprintf("The perf filter results count(%d) > 1", len(counters)))
	}

	return int64(counters[0].Value)
}

func GetNodeStat(perfClient *aggregate.PerfClient) map[string]*aggregate.NodeStat {
	var nodesStats = make(map[string]*aggregate.NodeStat)

	nodes := perfClient.GetNodeStats("replica")
	for _, node := range nodes {
		for name, value := range node.Stats {
			name = getCounterName(name)
			if nodesStats[node.Addr] == nil {
				nodesStats[node.Addr] = &aggregate.NodeStat{
					Addr:  node.Addr,
					Stats: make(map[string]float64),
				}
			}
			nodesStats[node.Addr].Stats[name] += value
		}
	}
	return nodesStats
}

func getCounterName(name string) string {
	ret := strings.Split(name, "@")
	if len(ret) != 0 {
		return ret[0]
	}
	return name
}
