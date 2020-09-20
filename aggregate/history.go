package aggregate

import (
	"container/list"
	"sync"
)

// TableStatsHistory is a time-ordered queue of TableStats.
type tableStatsHistory struct {
	lock sync.RWMutex

	stats    *list.List
	capacity int
}

// Emit a TableStats to the history. Will remove the oldest record from history.
func (h *tableStatsHistory) emit(stat *TableStats) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.stats.Len() == h.capacity {
		h.stats.Remove(h.stats.Front())
	}
	h.stats.PushBack(stat)
}

func (h *tableStatsHistory) iterateHistory(stat *TableStats) {

}

func newTableStatsHistory(capacity int) *tableStatsHistory {
	return &tableStatsHistory{
		stats:    list.New(),
		capacity: capacity,
	}
}

//
type historyStore struct {
	lock sync.RWMutex

	tables map[int]*tableStatsHistory
}

var globalHistoryStore = &historyStore{
	tables: make(map[int]*tableStatsHistory),
}

func init() {
	AddHookAfterTableStatEmitted(func(stats []TableStats) {
		s := globalHistoryStore

		s.lock.Lock()
		defer s.lock.Unlock()
		for _, stat := range stats {
			history, found := s.tables[stat.AppID]
			if !found {
				history = newTableStatsHistory(5)
				s.tables[stat.AppID] = history
			}
			history.emit(&stat)
		}
	})
	AddHookAfterTableDropped(func(appID int) {
		s := globalHistoryStore

		s.lock.Lock()
		defer s.lock.Unlock()
		delete(s.tables, appID)
	})
}
