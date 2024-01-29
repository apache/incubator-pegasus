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

import "sync"

// HookAfterTableStatEmitted is a hook of event that new TableStats are generated.
// Each call of the hook handles a batch of tables.
type HookAfterTableStatEmitted func(stats []TableStats, allStats ClusterStats)

// AddHookAfterTableStatEmitted adds a hook of event that a new TableStats is generated.
func AddHookAfterTableStatEmitted(hk HookAfterTableStatEmitted) {
	m := &hooksManager
	m.lock.Lock()
	defer m.lock.Unlock()
	m.emittedHooks = append(m.emittedHooks, hk)
}

// HookAfterTableDropped is a hook of event that a table is dropped.
type HookAfterTableDropped func(appID int)

// AddHookAfterTableDropped adds a hook of event that a table is dropped.
func AddHookAfterTableDropped(hk HookAfterTableDropped) {
	m := &hooksManager
	m.lock.Lock()
	defer m.lock.Unlock()
	m.droppedHooks = append(m.droppedHooks, hk)
}

type tableStatsHooksManager struct {
	lock         sync.RWMutex
	emittedHooks []HookAfterTableStatEmitted
	droppedHooks []HookAfterTableDropped
}

func (m *tableStatsHooksManager) afterTableStatsEmitted(stats []TableStats, allStat ClusterStats) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, hook := range m.emittedHooks {
		hook(stats, allStat)
	}
}

func (m *tableStatsHooksManager) afterTableDropped(appID int32) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, hook := range m.droppedHooks {
		hook(int(appID))
	}
}

var hooksManager tableStatsHooksManager
