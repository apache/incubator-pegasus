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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddHook(t *testing.T) {
	var actualTableStats []TableStats
	var actualClusterStats ClusterStats
	AddHookAfterTableStatEmitted(func(stats []TableStats, allStats ClusterStats) {
		actualTableStats = stats
		actualClusterStats = allStats
	})

	t1 := TableStats{TableName: "test", Stats: map[string]float64{"write": 512.0}, Timestamp: time.Now()}
	t2 := TableStats{TableName: "stat", Stats: map[string]float64{"write": 256.0}, Timestamp: time.Now()}
	all := ClusterStats{Stats: map[string]float64{"write": 768.0}}

	ch := make(chan interface{})
	go func() {
		hooksManager.afterTableStatsEmitted([]TableStats{t1, t2}, all)
		ch <- nil
	}()
	<-ch

	assert.EqualValues(t, actualTableStats, []TableStats{t1, t2})
	assert.EqualValues(t, actualClusterStats, all)

	// clear up
	hooksManager = tableStatsHooksManager{}
}
