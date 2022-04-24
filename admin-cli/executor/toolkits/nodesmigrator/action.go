package nodesmigrator

import (
	"fmt"
	"sync"
)

type Action struct {
	from    *MigratorNode
	to      *MigratorNode
	replica *Replica
}

type MigrateActions struct {
	actionList map[string]*Action
}

var migrateActionsMu sync.Mutex

func (acts *MigrateActions) put(currentAction *Action) {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()

	acts.actionList[currentAction.toString()] = currentAction
}

func (acts *MigrateActions) exist(currentAction *Action) bool {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()

	for _, action := range acts.actionList {
		if action.replica.gpid.String() == currentAction.replica.gpid.String() {
			if action.to.node.String() == currentAction.to.node.String() ||
				action.from.node.String() == currentAction.from.node.String() {
				return true
			}
		}
	}
	return false
}

func (acts *MigrateActions) delete(currentAction *Action) {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()

	delete(acts.actionList, currentAction.toString())
}

func (acts *MigrateActions) getConcurrent(node *MigratorNode) int {
	migrateActionsMu.Lock()
	defer func() {
		migrateActionsMu.Unlock()
	}()

	var count = 0
	for _, act := range acts.actionList {
		if act.to.node.String() == node.String() {
			count++
		}
	}
	return count
}

func (act *Action) toString() string {
	return fmt.Sprintf("[%s]%s:%s=>%s", act.replica.operation.String(), act.replica.gpid.String(),
		act.from.node.String(), act.to.node.String())
}
