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
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	migrator "github.com/apache/incubator-pegasus/admin-cli/client"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/util"
)

type Replica struct {
	gpid      *base.Gpid
	operation migrator.BalanceType
}

type ReplicasSlice []*Replica

func (r ReplicasSlice) Len() int           { return len(r) }
func (r ReplicasSlice) Less(i, j int) bool { return r[i].operation == migrator.BalanceCopySec }
func (r ReplicasSlice) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type MigratorNode struct {
	node     *util.PegasusNode
	replicas ReplicasSlice
}

func (r *Replica) String() string {
	return fmt.Sprintf("%s|%s", r.gpid.String(), r.operation.String())
}

func (m *MigratorNode) downgradeAllReplicaToSecondary(client *executor.Client) {
	if m.checkIfNoPrimary(client) {
		return
	}

	for {
		err := migrator.MigratePrimariesOut(client.Meta, m.node)
		if err != nil {
			toolkits.LogDebug(fmt.Sprintf("migrate primary out of %s is invalid now, err = %s\n", m.String(), err))
			time.Sleep(10 * time.Second)
			continue
		}

		for {
			time.Sleep(10 * time.Second)
			if !m.checkIfNoPrimary(client) {
				continue
			}
			return
		}
	}
}

func (m *MigratorNode) checkIfNoPrimary(client *executor.Client) bool {
	tables, err := client.Meta.ListAvailableApps()
	if err != nil {
		toolkits.LogDebug(fmt.Sprintf("migrate primary out of %s is invalid when list app, err = %s", m.String(), err))
		return false
	}

	for _, tb := range tables {
		partitions, err := migrator.ListPrimariesOnNode(client.Meta, m.node, tb.AppName)
		if err != nil {
			toolkits.LogDebug(fmt.Sprintf("migrate primary out of %s is invalid when list primaries, err = %s", m.String(), err))
			return false
		}
		if len(partitions) > 0 {
			toolkits.LogDebug(fmt.Sprintf("migrate primary out of %s is not completed, current count = %d", m.String(), len(partitions)))
			return false
		}
	}

	toolkits.LogInfo(fmt.Sprintf("migrate primary out of %s successfully", m.String()))
	return true
}

func (m *MigratorNode) contain(gpid *base.Gpid) bool {
	for _, replica := range m.replicas {
		if replica.gpid.String() == gpid.String() {
			return true
		}
	}
	return false
}

func (m *MigratorNode) concurrent(acts *MigrateActions) int {
	return acts.getConcurrent(m)
}

/* todo(jiashuo1) maybe need delete
func (m *MigratorNode) primaryCount() int {
	count := 0
	for _, replica := range m.replicas {
		if replica.operation == migrator.BalanceCopyPri {
			count++
		}
	}
	return count
}

func (m *MigratorNode) secondaryCount() int {
	count := 0
	for _, replica := range m.replicas {
		if replica.operation == migrator.BalanceCopySec {
			count++
		}
	}
	return count
}*/

func (m *MigratorNode) String() string {
	return m.node.String()
}
