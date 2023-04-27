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
	"container/list"
	"sync"
)

const (
	historyMaxCapacity = 10
)

// threadSafeHistory is a time-ordered queue of stats.
type threadSafeHistory struct {
	lock sync.RWMutex

	stats    *list.List
	capacity int
}

// Emit a TableStats to the history. Will remove the oldest record from history.
func (h *threadSafeHistory) emit(stat interface{}) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.stats.Len() == h.capacity {
		h.stats.Remove(h.stats.Front())
	}
	h.stats.PushBack(stat)
}

func newHistory(capacity int) *threadSafeHistory {
	return &threadSafeHistory{
		stats:    list.New(),
		capacity: capacity,
	}
}

type historyStore struct {
	lock sync.RWMutex

	tables  map[int]*threadSafeHistory
	cluster *threadSafeHistory
}

var globalHistoryStore = &historyStore{
	tables:  make(map[int]*threadSafeHistory),
	cluster: newHistory(historyMaxCapacity),
}

// SnapshotClusterStats takes a snapshot from the history. The returned array is ordered by time.
func SnapshotClusterStats() []ClusterStats {
	s := globalHistoryStore

	s.lock.RLock()
	defer s.lock.RUnlock()

	var result []ClusterStats
	l := s.cluster.stats
	for e := l.Front(); e != nil; e = e.Next() {
		stat, _ := e.Value.(*ClusterStats)
		result = append(result, *stat)
	}
	return result
}

func init() {
	initHistoryStore()
}

func initHistoryStore() {
	AddHookAfterTableStatEmitted(func(stats []TableStats, allStat ClusterStats) {
		s := globalHistoryStore

		s.lock.Lock()
		defer s.lock.Unlock()
		for _, stat := range stats {
			history, found := s.tables[stat.AppID]
			if !found {
				history = newHistory(historyMaxCapacity)
				s.tables[stat.AppID] = history
			}
			history.emit(&stat)
		}
		s.cluster.emit(&allStat)
	})

	AddHookAfterTableDropped(func(appID int) {
		s := globalHistoryStore

		s.lock.Lock()
		defer s.lock.Unlock()
		delete(s.tables, appID)
	})
}
