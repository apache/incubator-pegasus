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

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/pegasus-kv/admin-cli/util"
)

// fakeMeta implements the Meta interface
type fakeMeta struct {
	availableApps []*admin.AppInfo

	metaLevel admin.MetaFunctionLevel
}

func (m *fakeMeta) Close() error {
	return nil
}

func (m *fakeMeta) ListAvailableApps() ([]*admin.AppInfo, error) {
	return m.availableApps, nil
}

func (m *fakeMeta) ListApps(admin.AppStatus) ([]*admin.AppInfo, error) {
	panic("unimplemented")
}

func (m *fakeMeta) QueryConfig(tbName string) (*replication.QueryCfgResponse, error) {
	resp := &replication.QueryCfgResponse{}

	for _, app := range m.availableApps {
		if app.AppName != tbName {
			continue
		}

		resp.AppID = app.AppID
		resp.PartitionCount = app.PartitionCount
		resp.Partitions = make([]*replication.PartitionConfiguration, app.PartitionCount)
		for i := range resp.Partitions {
			resp.Partitions[i] = &replication.PartitionConfiguration{
				Pid:             &base.Gpid{Appid: app.AppID, PartitionIndex: int32(i)},
				Primary:         &base.RPCAddress{}, // even the primary is unavailable, it should still not be nil
				MaxReplicaCount: 3,
			}
		}

		for _, n := range fakePegasusCluster.nodes {
			for pri := range n.primaries {
				if pri.Appid == app.AppID {
					pc := resp.Partitions[int(pri.PartitionIndex)]
					pc.Primary = n.n.RPCAddress()
				}
			}
			for sec := range n.secondaries {
				if sec.Appid == app.AppID {
					pc := resp.Partitions[int(sec.PartitionIndex)]
					pc.Secondaries = append(pc.Secondaries, n.n.RPCAddress())
				}
			}
		}
		return resp, nil
	}

	resp.Err = &base.ErrorCode{Errno: base.ERR_OBJECT_NOT_FOUND.String()}
	return resp, nil
}

func (m *fakeMeta) MetaControl(lvl admin.MetaFunctionLevel) (oldLevel admin.MetaFunctionLevel, err error) {
	m.metaLevel = lvl
	return m.metaLevel, nil
}

func (m *fakeMeta) QueryClusterInfo() (map[string]string, error) {
	panic("unimplemented")
}

func (m *fakeMeta) UpdateAppEnvs(string, map[string]string) error {
	panic("unimplemented")
}

func (m *fakeMeta) ClearAppEnvs(string, string) error {
	panic("unimplemented")
}

func (m *fakeMeta) DelAppEnvs(string, []string) error {
	panic("unimplemented")
}

func (m *fakeMeta) CreateApp(tbName string, envs map[string]string, partitionNum int) (int32, error) {
	appID := len(m.availableApps)
	m.availableApps = append(m.availableApps, &admin.AppInfo{
		AppID:          int32(appID),
		AppName:        tbName,
		Envs:           envs,
		PartitionCount: int32(partitionNum),
		Status:         admin.AppStatus_AS_AVAILABLE,
	})
	return int32(appID), nil
}

func (m *fakeMeta) DropApp(string, int64) error {
	panic("unimplemented")
}

func (m *fakeMeta) ModifyDuplication(string, int, admin.DuplicationStatus) error {
	panic("unimplemented")
}

func (m *fakeMeta) AddDuplication(string, string, bool) (*admin.DuplicationAddResponse, error) {
	panic("unimplemented")
}

func (m *fakeMeta) QueryDuplication(string) (*admin.DuplicationQueryResponse, error) {
	panic("unimplemented")
}

func (m *fakeMeta) ListNodes() ([]*admin.NodeInfo, error) {
	var result []*admin.NodeInfo
	for _, n := range fakePegasusCluster.nodes {
		result = append(result, &admin.NodeInfo{
			Status:  admin.NodeStatus_NS_ALIVE,
			Address: base.NewRPCAddress(n.n.IP, n.n.Port),
		})
	}
	return result, nil
}

func (m *fakeMeta) RecallApp(int, string) (*admin.AppInfo, error) {
	panic("unimplemented")
}

func (m *fakeMeta) Balance(pid *base.Gpid, opType BalanceType, from *util.PegasusNode, to *util.PegasusNode) error {
	fakeFrom := findNodeInFakeCluster(from)
	fakeTo := findNodeInFakeCluster(to)

	switch opType {
	case BalanceMovePri:
		delete(fakeFrom.primaries, *pid)
		fakeTo.primaries[*pid] = true
		delete(fakeTo.secondaries, *pid)
		fakeFrom.secondaries[*pid] = true
	default:
		panic("unimplemented")
	}
	return nil
}

func (m *fakeMeta) Propose(pid *base.Gpid, opType admin.ConfigType, target *util.PegasusNode, node *util.PegasusNode) error {
	fakeNode := findNodeInFakeCluster(node)

	switch opType {
	case admin.ConfigType_CT_DOWNGRADE_TO_INACTIVE:
		delete(fakeNode.primaries, *pid)
		delete(fakeNode.secondaries, *pid)
	default:
		panic("unimplemented")
	}
	return nil
}

func findNodeInFakeCluster(pn *util.PegasusNode) *fakeNode {
	var ret *fakeNode
	for _, n := range fakePegasusCluster.nodes {
		if n.n.TCPAddr() == pn.TCPAddr() {
			ret = n
		}
	}
	if ret == nil {
		panic(fmt.Sprintf("no corresponding node %s", pn.TCPAddr()))
	}
	return ret
}

func (m *fakeMeta) StartBackupApp(tableID int, providerType string, backupPath string) (*admin.StartBackupAppResponse, error) {
	panic("unimplemented")
}

func (m *fakeMeta) QueryBackupStatus(tableID int, backupID int64) (*admin.QueryBackupStatusResponse, error) {
	panic("unimplemented")
}

func (m *fakeMeta) RestoreApp(oldClusterName string, oldTableName string, oldTableID int, backupID int64, providerType string,
	newTableName string, restorePath string, skipBadPartition bool, policyName string) (*admin.CreateAppResponse, error) {
	panic("unimplemented")
}

func (m *fakeMeta) StartPartitionSplit(tableName string, newPartitionCount int) error {
	panic("unimplemented")
}

func (m *fakeMeta) QuerySplitStatus(tableName string) (*admin.QuerySplitResponse, error) {
	panic("unimplemented")
}

func (m *fakeMeta) PausePartitionSplit(tableName string, parentPidx int) error {
	panic("unimplemented")
}

func (m *fakeMeta) RestartPartitionSplit(tableName string, parentPidx int) error {
	panic("unimplemented")
}

func (m *fakeMeta) CancelPartitionSplit(tableName string, oldPartitionCount int) error {
	panic("unimplemented")
}
