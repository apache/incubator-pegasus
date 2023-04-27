// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package aggregate

import (
	"strconv"
	"strings"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
)

type partitionPerfCounter struct {
	name  string
	gpid  base.Gpid
	value float64
}

// decodePartitionPerfCounter implements the v1 version of metric decoding.
func decodePartitionPerfCounter(name string, value float64) *partitionPerfCounter {
	idx := strings.LastIndex(name, "@")
	gpidStr := name[idx+1:]
	appIDAndPartitionID := strings.Split(gpidStr, ".")
	if len(appIDAndPartitionID) != 2 {
		// special case: in some mis-desgined metrics, what follows after a '@' may not be a replica id
		return nil
	}
	appIDAndPartitionID = appIDAndPartitionID[:2] // "AppID.PartitionIndex"
	appID, err := strconv.Atoi(appIDAndPartitionID[0])
	if err != nil {
		return nil
	}
	partitionIndex, err := strconv.Atoi(appIDAndPartitionID[1])
	if err != nil {
		return nil
	}
	return &partitionPerfCounter{
		name: name[:idx], // strip out the replica id
		gpid: base.Gpid{
			Appid:          int32(appID),
			PartitionIndex: int32(partitionIndex),
		},
		value: value,
	}
}

// TODO(wutao1): implement the v2 version of metric decoding according to
// https://github.com/apache/incubator-pegasus/blob/master/rfcs/2020-08-27-metric-api.md
