package aggregate

import (
	"container/list"
	"sync"
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

//
type historyStore struct {
	lock sync.RWMutex

	tables  map[int]*threadSafeHistory
	cluster *threadSafeHistory
}

var globalHistoryStore = &historyStore{
	tables:  make(map[int]*threadSafeHistory),
	cluster: newHistory(5),
}

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
				history = newHistory(10)
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
