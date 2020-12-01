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

package helper

import (
	"fmt"

	"github.com/pegasus-kv/collector/aggregate"
)

func GetReplicaCounterValue(perfClient *aggregate.PerfSession, counter string, gpid string) int64 {
	counters, err := perfClient.GetPerfCounters(fmt.Sprintf("%s@%s", counter, gpid))
	if err != nil {
		return -1
	}

	if len(counters) == 1 {
		return int64(counters[0].Value)
	}
	return -1
}

func GetNodeCounterValue(perfClient *aggregate.PerfSession, counter string) int64 {
	counters, err := perfClient.GetPerfCounters(counter)
	if err != nil {
		return -1
	}

	if len(counters) == 1 {
		return int64(counters[0].Value)
	}
	return -1
}

func GetNodeAggregateCounterValue(perfClient *aggregate.PerfSession, counter string) int64 {
	counters, err := perfClient.GetPerfCounters(counter)
	if err != nil {
		return -1
	}

	var value float64
	for _, counter := range counters {
		value += counter.Value
	}
	return int64(value)
}
