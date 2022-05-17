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

package nodesbalancer

import (
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/apache/incubator-pegasus/admin-cli/executor"
	"github.com/apache/incubator-pegasus/admin-cli/executor/toolkits"
)

// By default, the node capacity of the server needs to be updated every 10 minutes.
// Therefore, after a partition is migrated completed, the tool cannot immediately
// obtain the latest capacity distribution. Please adjust the node capacity update
// interval of the server to speed up the equalization speed. Relevant configurations
// are as follows:
//
//- disk_stat_interval_seconds = 600
//+ disk_stat_interval_seconds = 60 # or less
//
//- gc_memory_replica_interval_ms = 600000
//+ gc_memory_replica_interval_ms = 60000 # or less

func BalanceNodeCapacity(client *executor.Client, auto bool) error {
	err := initClusterEnv(client)
	if err != nil {
		return err
	}

	balancer := &Migrator{}
	for {
		err := balancer.updateNodesLoad(client)
		if err != nil {
			toolkits.LogInfo(fmt.Sprintf("retry update load, err = %s", err.Error()))
			time.Sleep(time.Second * 10)
			continue
		}

		action, err := balancer.selectNextAction(client)
		if err != nil {
			return err
		}

		err = client.Meta.Balance(action.replica.Gpid, action.replica.Status, action.from.Node, action.to.Node)
		if err != nil {
			return fmt.Errorf("migrate action[%s] now is invalid: %s", action.toString(), err.Error())
		}
		err = waitCompleted(client, action)
		if err != nil {
			return fmt.Errorf("wait replica migrate err: %s", err.Error())
		}
		if !auto {
			break
		}
		time.Sleep(time.Second * 10)
	}
	return nil
}

func initClusterEnv(client *executor.Client) error {
	toolkits.LogWarn("This cluster will be balanced based capacity, please don't open count-balance in later")
	time.Sleep(time.Second * 3)

	// set meta level as steady
	err := executor.SetMetaLevel(client, "steady")
	if err != nil {
		return err
	}
	// disable migrate replica base `lively`
	toolkits.LogInfo("set meta.lb.only_move_primary true")
	err = executor.RemoteCommand(client, session.NodeTypeMeta, "", "meta.lb.only_move_primary", []string{"true"})
	if err != nil {
		return err
	}
	toolkits.LogInfo("set meta.lb.only_primary_balancer true")
	err = executor.RemoteCommand(client, session.NodeTypeMeta, "", "meta.lb.only_primary_balancer", []string{"true"})
	if err != nil {
		return err
	}
	// reset garbage replica clear interval
	toolkits.LogInfo("set gc_disk_error_replica_interval_seconds 10")
	err = executor.ConfigCommand(client, session.NodeTypeReplica, "", "gc_disk_error_replica_interval_seconds", "set", "10")
	if err != nil {
		return err
	}
	toolkits.LogInfo("set gc_disk_garbage_replica_interval_seconds 10")
	err = executor.ConfigCommand(client, session.NodeTypeReplica, "", "gc_disk_garbage_replica_interval_seconds", "set", "10")
	if err != nil {
		return err
	}
	return nil
}
