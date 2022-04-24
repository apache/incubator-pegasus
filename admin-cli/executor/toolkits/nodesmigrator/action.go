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
