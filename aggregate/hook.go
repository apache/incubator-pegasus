package aggregate

import "sync"

// HookAfterTableStatEmitted is a hook of event that a new TableStats is generated.
type HookAfterTableStatEmitted func(stat TableStats)

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

func (m *tableStatsHooksManager) afterTablStatsEmitted(stat TableStats) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, hook := range m.emittedHooks {
		hook(stat)
	}
}

func (m *tableStatsHooksManager) afterTableDropped(appID int) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	for _, hook := range m.droppedHooks {
		hook(appID)
	}
}

var hooksManager tableStatsHooksManager
