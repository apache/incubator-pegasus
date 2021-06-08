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

package executor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/radmin"
	"github.com/XiaoMi/pegasus-go-client/session"
	adminClient "github.com/pegasus-kv/admin-cli/client"
	"github.com/pegasus-kv/admin-cli/util"
)

func DiskMigrate(client *Client, replicaServer string, pidStr string, from string, to string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	pid, err := util.Str2Gpid(pidStr)
	if err != nil {
		return err
	}

	node, err := client.Nodes.GetNode(replicaServer, session.NodeTypeReplica)
	if err != nil {
		return err
	}
	replica := node.Replica()

	resp, err := replica.DiskMigrate(ctx, &radmin.ReplicaDiskMigrateRequest{
		Pid:        pid,
		OriginDisk: from,
		TargetDisk: to,
	})

	if err != nil {
		if resp != nil && resp.Hint != nil {
			return fmt.Errorf("Internal server error [%s:%s]", err, *resp.Hint)
		}
		return err
	}

	return nil
}

const (
	WaitRunning  = time.Second * 10 // time for wait migrate complete
	WaitCleaning = time.Second * 90 // time for wait garbage replica to clean complete
)

// auto balance target node disk usage:
// -1. change the pegasus server disk cleaner internal for clean temp replica to free disk space in time
// -2. get the optimal migrate action to be ready to balance the disk until can't migrate base latest disk space stats
// -3. if current replica is `primary` status, force assign the replica to `secondary` status
// -4. migrate the replica base `getNextMigrateAction` result
// -5. loop query migrate progress using `DiskMigrate`, it will response `ERR_BUSY` if running
// -6. start next loop until can't allow to balance the node
// -7. recover disk cleaner internal if balance complete
// -8. set meta status to `lively` to balance primary and secondary // TODO(jiashuo1)
func DiskBalance(client *Client, replicaServer string, minSize int64, auto bool) error {
	if err := changeDiskCleanerInterval(client, replicaServer, 1); err != nil {
		return err
	}
	defer func() {
		if err := changeDiskCleanerInterval(client, replicaServer, 86400); err != nil {
			fmt.Println("revert disk cleaner failed")
		}
	}()

	for {
		action, err := getNextMigrateAction(client, replicaServer, minSize)
		if err != nil {
			return err
		}
		if action.replica.Status != "secondary" {
			err := forceAssignReplicaToSecondary(client, replicaServer, action.replica.Gpid)
			if err != nil {
				return err
			}
			time.Sleep(WaitRunning)
			continue
		}
		err = DiskMigrate(client, replicaServer, action.replica.Gpid, action.from, action.to)
		if err == nil {
			fmt.Printf("migrate(%s) has started, wait complete...\n", action.toString())
			for {
				// TODO(jiashuo1): using DiskMigrate RPC to query status, consider support queryDiskMigrateStatus RPC
				err = DiskMigrate(client, replicaServer, action.replica.Gpid, action.from, action.to)
				if err == nil {
					time.Sleep(WaitRunning)
					continue
				}

				if strings.Contains(err.Error(), base.ERR_BUSY.String()) {
					fmt.Printf("migrate(%s) is running, msg=%s, wait complete...\n", action.toString(), err.Error())
					time.Sleep(WaitRunning)
					continue
				}
				fmt.Printf("migrate(%s) is completed，result=%s, wait disk cleaner remove garbage...\n\n", action.toString(), err.Error())
				break
			}
			time.Sleep(WaitCleaning)
			continue
		}
		if auto {
			time.Sleep(WaitCleaning)
			continue
		}
		break
	}

	return nil
}

type DiskStats struct {
	DiskCapacity    DiskCapacityStruct
	ReplicaCapacity []ReplicaCapacityStruct
}

type MigrateDisk struct {
	AverageUsage int64
	HighDisk     DiskStats
	LowDisk      DiskStats
}

type MigrateAction struct {
	node    string
	replica ReplicaCapacityStruct
	from    string
	to      string
}

func (m *MigrateAction) toString() string {
	return fmt.Sprintf("node=%s, replica=%s, %s=>%s", m.node, m.replica.Gpid, m.from, m.to)
}

func changeDiskCleanerInterval(client *Client, replicaServer string, cleanInterval int64) error {
	fmt.Printf("set gc_disk_migration_origin_replica_interval_seconds = %ds ", cleanInterval)
	err := ConfigCommand(client, session.NodeTypeReplica, replicaServer,
		"gc_disk_migration_origin_replica_interval_seconds", "set", cleanInterval)
	return err
}

func getNextMigrateAction(client *Client, replicaServer string, minSize int64) (*MigrateAction, error) {
	disks, totalUsage, totalCapacity, err := queryDiskCapacityInfo(client, replicaServer)
	if err != nil {
		return nil, err
	}
	diskMigrateInfo, err := getMigrateDiskInfo(client, replicaServer, disks, totalUsage, totalCapacity)
	if err != nil {
		return nil, err
	}

	migrateAction, err := computeMigrateAction(diskMigrateInfo, minSize)
	if err != nil {
		return nil, err
	}
	return migrateAction, nil
}

func forceAssignReplicaToSecondary(client *Client, replicaServer string, gpid string) error {
	fmt.Printf("WARNING: the select replica is not secondary, will force assign it secondary\n")
	if _, err := client.Meta.MetaControl(admin.MetaFunctionLevel_fl_steady); err != nil {
		return err
	}
	secondaryNode, err := getReplicaSecondaryNode(client, gpid)
	if err != nil {
		return err
	}
	replica, err := util.Str2Gpid(gpid)
	if err != nil {
		return err
	}
	return client.Meta.Balance(replica, adminClient.BalanceMovePri,
		util.NewNodeFromTCPAddr(replicaServer, session.NodeTypeReplica), secondaryNode)
}

// todo(jiashuo1) next pr
func getReplicaSecondaryNode(client *Client, gpid string) (*util.PegasusNode, error) {
	return nil, nil
}

// todo(jiashuo1) next pr
func queryDiskCapacityInfo(client *Client, replicaServer string) ([]DiskCapacityStruct, int64, int64, error) {
	return nil, 0, 0, nil
}

// todo(jiashuo1) next pr
func getMigrateDiskInfo(client *Client, replicaServer string, disks []DiskCapacityStruct,
	totalUsage int64, totalCapacity int64) (*MigrateDisk, error) {
	return nil, nil
}

// todo(jiashuo1) next pr
func computeMigrateAction(migrate *MigrateDisk, minSize int64) (*MigrateAction, error) {
	return nil, nil
}
