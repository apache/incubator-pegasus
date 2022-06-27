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
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
	"github.com/apache/incubator-pegasus/admin-cli/util"
	"github.com/apache/incubator-pegasus/go-client/idl/admin"
)

var GlobalBatchTable = false
var GlobalBatchTarget = false

func MigrateAllReplicaToNodes(client *executor.Client, from []string, to []string, tables []string, batchTable bool,
	batchTarget bool, targetCount int, concurrent int) error {
	GlobalBatchTable = batchTable
	GlobalBatchTarget = batchTarget
	if len(to) != targetCount {
		toolkits.LogPanic(fmt.Sprintf("please make sure the targets count == `--final_target` value： %d vs %d", len(to),
			targetCount))
	}

	toolkits.LogWarn(fmt.Sprintf("you now migrate to target count assign to be %d in final, "+
		"please make sure it is ok! sleep 10s and then start", targetCount))
	time.Sleep(time.Second * 10)

	nodesMigrator, err := createNewMigrator(client, from, to)
	if err != nil {
		return err
	}

	var tableList []string
	if len(tables) != 0 && tables[0] != "" {
		tableList = tables
	} else {
		tbs, err := client.Meta.ListApps(admin.AppStatus_AS_AVAILABLE)
		if err != nil {
			return fmt.Errorf("list app failed: %s", err.Error())
		}
		for _, tb := range tbs {
			tableList = append(tableList, tb.AppName)
		}
	}

	currentOriginNode := nodesMigrator.selectNextOriginNode()
	currentTargetNodes := nodesMigrator.targets
	firstOrigin := currentOriginNode
	var totalRemainingReplica int32 = math.MaxInt32
	originRound := -1
	runTargetCount := 0

	balanceFactor := 0
	for {
		if totalRemainingReplica <= 0 {
			toolkits.LogInfo("\n\n==============completed for all origin nodes has been migrated================")
			return executor.ListNodes(client)
		}

		if currentOriginNode.String() == firstOrigin.String() {
			originRound++
		}
		toolkits.LogInfo(fmt.Sprintf("\n\n*******************[%d|%s]start migrate replicas, remainingReplica=%d*****************",
			balanceFactor, currentOriginNode.String(), totalRemainingReplica))

		currentOriginNode.downgradeAllReplicaToSecondary(client)
		if !GlobalBatchTarget {
			runTargetCount++
			target := nodesMigrator.selectNextTargetNode(nodesMigrator.targets)
			target.downgradeAllReplicaToSecondary(client)
			currentTargetNodes = []*util.PegasusNode{target.node}
		}

		if !GlobalBatchTarget {
			if runTargetCount/targetCount >= 1 {
				runTargetCount = 0
				balanceFactor++
			}
		} else {
			balanceFactor = originRound
		}

		totalRemainingReplica = 0
		tableCount := len(tableList)
		var wg sync.WaitGroup
		wg.Add(tableCount)
		for _, tb := range tableList {
			targetTable := tb
			if GlobalBatchTable {
				go func() {
					worker, _ := createNewMigrator(client, from, to)
					remainingCount := worker.run(client, targetTable, balanceFactor, currentOriginNode, currentTargetNodes, concurrent)
					atomic.AddInt32(&totalRemainingReplica, int32(remainingCount))
					wg.Done()
				}()
			} else {
				remainingCount := nodesMigrator.run(client, targetTable, balanceFactor, currentOriginNode, currentTargetNodes, concurrent)
				atomic.AddInt32(&totalRemainingReplica, int32(remainingCount))
				wg.Done()
			}
		}
		wg.Wait()
		currentOriginNode = nodesMigrator.selectNextOriginNode()
		time.Sleep(10 * time.Second)
	}
}
