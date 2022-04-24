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

package client

import (
	"fmt"
	"math/rand"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/admin-cli/util"
	log "github.com/sirupsen/logrus"
)

func SetMetaLevelLively(meta Meta) error {
	_, err := meta.MetaControl(admin.MetaFunctionLevel_fl_lively)
	return err
}

func SetMetaLevelSteady(meta Meta) error {
	_, err := meta.MetaControl(admin.MetaFunctionLevel_fl_steady)
	return err
}

func listReplicasOnNode(meta Meta, node *util.PegasusNode, tableName string) ([]*replication.PartitionConfiguration, error) {
	resp, err := meta.QueryConfig(tableName)
	if err != nil {
		return nil, err
	}

	var result []*replication.PartitionConfiguration
	for _, part := range resp.Partitions {
		if part.Primary.GetAddress() == node.TCPAddr() {
			result = append(result, part)
		}
		for _, sec := range part.Secondaries {
			if sec.GetAddress() == node.TCPAddr() {
				result = append(result, part)
				break
			}
		}
	}
	return result, nil
}

func ListPrimariesOnNode(meta Meta, node *util.PegasusNode, tableName string) ([]*replication.PartitionConfiguration, error) {
	resp, err := meta.QueryConfig(tableName)
	if err != nil {
		return nil, err
	}

	var result []*replication.PartitionConfiguration
	for _, part := range resp.Partitions {
		if part.Primary.GetAddress() == node.TCPAddr() {
			result = append(result, part)
		}
	}
	return result, nil
}

func replicaNode(addr *base.RPCAddress) *util.PegasusNode {
	return util.NewNodeFromTCPAddr(addr.GetAddress(), session.NodeTypeReplica)
}

// MigratePrimariesOut migrates all primaries out from the specified node.
// Internally, for every partition it merely swaps the roles of primary and secondary,
// so it incurs no data migration.
// Eventually, the node will have no primaries because they are all turned to secondaries.
func MigratePrimariesOut(meta Meta, node *util.PegasusNode) error {
	cmd := fmt.Sprintf("MigratePrimariesOut from=%s", node.CombinedAddr())
	log.Debug(cmd)

	if err := SetMetaLevelSteady(meta); err != nil {
		return fmt.Errorf("%s failed: %s", cmd, err)
	}

	tables, err := meta.ListAvailableApps()
	if err != nil {
		return fmt.Errorf("%s failed: %s", cmd, err)
	}

	for _, tb := range tables {
		tbCmd := cmd + fmt.Sprintf(" table=%s", tb.AppName)
		log.Debug(tbCmd)

		partitions, err := ListPrimariesOnNode(meta, node, tb.AppName)
		if err != nil {
			return fmt.Errorf("%s failed: %s", tbCmd, err)
		}
		for _, part := range partitions {
			from := node

			secIdx := rand.Intn(len(part.Secondaries))
			sec := part.Secondaries[secIdx]
			to := replicaNode(sec)

			balanceCmd := tbCmd + fmt.Sprintf(" to=%s gpid=%s", to.CombinedAddr(), part.Pid)
			log.Debug(balanceCmd)

			err := meta.Balance(part.Pid, BalanceMovePri, from, to)
			if err != nil {
				return fmt.Errorf("%s failed: %s", balanceCmd, err)
			}
		}
	}
	return nil
}

// DowngradeNode sets all secondaries from the specified node to inactive state.
// NOTE: this step requires that the node has no primary, otherwise error is returned.
func DowngradeNode(meta Meta, node *util.PegasusNode) error {
	return downgradeNode(meta, node, nil)
}

// DowngradeNodeWithDetails is like DowngradeNode but also returns the partitions that were downgraded.
func DowngradeNodeWithDetails(meta Meta, node *util.PegasusNode) ([]*base.Gpid, error) {
	var downgradedParts []*base.Gpid
	err := downgradeNode(meta, node, &downgradedParts)
	if err != nil {
		return nil, err
	}
	return downgradedParts, nil
}

func downgradeNode(meta Meta, node *util.PegasusNode, downgradedParts *[]*base.Gpid) error {
	cmd := fmt.Sprintf("DowngradeNode node=%s", node.CombinedAddr())
	log.Debug(cmd)

	if err := SetMetaLevelSteady(meta); err != nil {
		return fmt.Errorf("%s failed: %s", cmd, err)
	}

	tables, err := meta.ListAvailableApps()
	if err != nil {
		return fmt.Errorf("%s failed: %s", cmd, err)
	}

	for _, tb := range tables {
		tbCmd := cmd + fmt.Sprintf(" table=%s", tb.AppName)
		log.Debug(tbCmd)

		partitions, err := listReplicasOnNode(meta, node, tb.AppName)
		if err != nil {
			return fmt.Errorf("%s failed: %s", tbCmd, err)
		}
		for _, part := range partitions {
			if part.Primary.GetAddress() == node.TCPAddr() {
				return fmt.Errorf("%s failed: no primary should be on this node", tbCmd)
			}

			pri := replicaNode(part.Primary)
			proposeCmd := tbCmd + fmt.Sprintf(" target=%s gpid=%s", pri.CombinedAddr(), part.Pid)
			log.Debug(proposeCmd)

			err := meta.Propose(part.Pid, admin.ConfigType_CT_DOWNGRADE_TO_INACTIVE, pri, node)
			if err != nil {
				return fmt.Errorf("%s failed: %s", proposeCmd, err)
			}

			if downgradedParts != nil {
				*downgradedParts = append(*downgradedParts, part.Pid)
			}
		}
	}
	return nil
}
