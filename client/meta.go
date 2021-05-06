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
	"context"
	"fmt"
	"reflect"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/pegasus-kv/admin-cli/util"
)

type BalanceType int

const (
	BalanceMovePri BalanceType = iota
	BalanceCopyPri
	BalanceCopySec
)

func (t BalanceType) String() string {
	switch t {
	case BalanceMovePri:
		return "MovePri"
	case BalanceCopyPri:
		return "CopyPri"
	case BalanceCopySec:
		return "CopySec"
	default:
		panic(fmt.Sprintf("unexpected BalanceType: %d", t))
	}
}

// Meta is a helper over pegasus-go-client's primitive session.MetaManager.
// It aims to provide an easy-to-use API that eliminates some boilerplate code, like
// context creation, request/response creation, etc.
type Meta interface {
	Close() error

	// ListAvailableApps lists only available tables.
	ListAvailableApps() ([]*admin.AppInfo, error)

	ListApps(status admin.AppStatus) ([]*admin.AppInfo, error)

	QueryConfig(tableName string) (*replication.QueryCfgResponse, error)

	MetaControl(level admin.MetaFunctionLevel) (oldLevel admin.MetaFunctionLevel, err error)

	QueryClusterInfo() (map[string]string, error)

	UpdateAppEnvs(tableName string, envs map[string]string) error

	ClearAppEnvs(tableName string, clearPrefix string) error

	DelAppEnvs(tableName string, keys []string) error

	CreateApp(tableName string, envs map[string]string, partitionCount int) (int32, error)

	DropApp(tableName string, reserveSeconds int64) error

	ModifyDuplication(tableName string, dupid int, status admin.DuplicationStatus) error

	AddDuplication(tableName string, remoteCluster string, freezed bool) (*admin.DuplicationAddResponse, error)

	QueryDuplication(tableName string) (*admin.DuplicationQueryResponse, error)

	ListNodes() ([]*admin.NodeInfo, error)

	RecallApp(originTableID int, newTableName string) (*admin.AppInfo, error)

	Balance(gpid *base.Gpid, opType BalanceType, from *util.PegasusNode, to *util.PegasusNode) error

	Propose(gpid *base.Gpid, action admin.ConfigType, target *util.PegasusNode, node *util.PegasusNode) error
}

type rpcBasedMeta struct {
	meta *session.MetaManager
}

// NewRPCBasedMeta creates the connection to meta.
func NewRPCBasedMeta(metaAddrs []string) Meta {
	return &rpcBasedMeta{
		meta: session.NewMetaManager(metaAddrs, session.NewNodeSession),
	}
}

// Some responses have not only error-code but also a string-type "hint" that can tells the error details.
// This function wraps the "hint" into error.
func wrapHintIntoError(hint string, err error) error {
	if err != nil {
		if hint != "" {
			return fmt.Errorf("%s [hint: %s]", err, hint)
		}
	}
	return err
}

func (m *rpcBasedMeta) Close() error {
	return m.meta.Close()
}

// `callback` always accepts non-nil `resp`.
func (m *rpcBasedMeta) callMeta(methodName string, req interface{}, callback func(resp interface{})) error {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()

	ret := reflect.ValueOf(m.meta).MethodByName(methodName).Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(req),
	})

	// the last returned value is always error
	ierr := ret[len(ret)-1].Interface()
	var err error
	if ierr != nil {
		err = ierr.(error)
	}

	if len(ret) == 1 {
		return err
	}

	// len(ret) == 2
	if !ret[0].IsNil() {
		callback(ret[0].Interface())
	}
	return err
}

func (m *rpcBasedMeta) ListAvailableApps() ([]*admin.AppInfo, error) {
	return m.ListApps(admin.AppStatus_AS_AVAILABLE)
}

func (m *rpcBasedMeta) ListApps(status admin.AppStatus) ([]*admin.AppInfo, error) {
	var result []*admin.AppInfo
	req := &admin.ListAppsRequest{Status: status}
	err := m.callMeta("ListApps", req, func(resp interface{}) {
		result = resp.(*admin.ListAppsResponse).Infos
	})
	return result, err
}

func (m *rpcBasedMeta) QueryConfig(tableName string) (*replication.QueryCfgResponse, error) {
	var result *replication.QueryCfgResponse
	err := m.callMeta("QueryConfig", tableName, func(resp interface{}) {
		result = resp.(*replication.QueryCfgResponse)
	})
	if err == nil {
		if result.GetErr().Errno == base.ERR_OBJECT_NOT_FOUND.String() {
			return nil, fmt.Errorf("table(%s) doesn't exist", tableName)
		}
		if result.GetErr().Errno != base.ERR_OK.String() {
			return nil, fmt.Errorf("query config failed: %s", result.GetErr())
		}
	}
	return result, err
}

func (m *rpcBasedMeta) MetaControl(level admin.MetaFunctionLevel) (oldLevel admin.MetaFunctionLevel, err error) {
	req := &admin.MetaControlRequest{Level: level}
	err = m.callMeta("MetaControl", req, func(resp interface{}) {
		oldLevel = resp.(*admin.MetaControlResponse).OldLevel
	})
	return oldLevel, err
}

func (m *rpcBasedMeta) QueryClusterInfo() (map[string]string, error) {
	result := make(map[string]string)
	req := &admin.ClusterInfoRequest{}
	err := m.callMeta("QueryClusterInfo", req, func(resp interface{}) {
		keys := resp.(*admin.ClusterInfoResponse).Keys
		values := resp.(*admin.ClusterInfoResponse).Values
		for i := range keys {
			result[keys[i]] = values[i]
		}
	})
	return result, err
}

func (m *rpcBasedMeta) updateAppEnvs(req *admin.UpdateAppEnvRequest) error {
	var hint string
	err := m.callMeta("UpdateAppEnv", req, func(resp interface{}) {
		hint = resp.(*admin.UpdateAppEnvResponse).HintMessage
	})
	return wrapHintIntoError(hint, err)
}

func (m *rpcBasedMeta) UpdateAppEnvs(tableName string, envs map[string]string) error {
	var keys []string
	var values []string
	for key, value := range envs {
		keys = append(keys, key)
		values = append(values, value)
	}
	req := &admin.UpdateAppEnvRequest{
		AppName: tableName,
		Op:      admin.AppEnvOperation_APP_ENV_OP_SET,
		Keys:    keys,
		Values:  values,
	}
	return m.updateAppEnvs(req)
}

func (m *rpcBasedMeta) ClearAppEnvs(tableName string, clearPrefix string) error {
	req := &admin.UpdateAppEnvRequest{
		AppName:     tableName,
		Op:          admin.AppEnvOperation_APP_ENV_OP_CLEAR,
		ClearPrefix: &clearPrefix,
	}
	return m.updateAppEnvs(req)
}

func (m *rpcBasedMeta) DelAppEnvs(tableName string, keys []string) error {
	req := &admin.UpdateAppEnvRequest{
		AppName: tableName,
		Op:      admin.AppEnvOperation_APP_ENV_OP_DEL,
		Keys:    keys,
	}
	return m.updateAppEnvs(req)
}

func (m *rpcBasedMeta) CreateApp(tableName string, envs map[string]string, partitionCount int) (int32, error) {
	var appID int32
	req := &admin.CreateAppRequest{
		AppName: tableName,
		Options: &admin.CreateAppOptions{
			PartitionCount: int32(partitionCount),
			Envs:           envs,

			// constants
			ReplicaCount: 3,
			IsStateful:   true,
			AppType:      "pegasus",
		},
	}
	err := m.callMeta("CreateApp", req, func(resp interface{}) {
		appID = resp.(*admin.CreateAppResponse).Appid
	})
	return appID, err
}

func (m *rpcBasedMeta) DropApp(tableName string, reserveSeconds int64) error {
	req := &admin.DropAppRequest{
		AppName: tableName,
		Options: &admin.DropAppOptions{
			SuccessIfNotExist: true,
			ReserveSeconds:    &reserveSeconds,
		},
	}
	err := m.callMeta("DropApp", req, func(resp interface{}) {})
	return err
}

func (m *rpcBasedMeta) ModifyDuplication(tableName string, dupid int, status admin.DuplicationStatus) error {
	req := &admin.DuplicationModifyRequest{
		AppName: tableName,
		Dupid:   int32(dupid),
		Status:  &status,
	}
	err := m.callMeta("ModifyDuplication", req, func(resp interface{}) {})
	return err
}

func (m *rpcBasedMeta) AddDuplication(tableName string, remoteCluster string, freezed bool) (*admin.DuplicationAddResponse, error) {
	var result *admin.DuplicationAddResponse
	req := &admin.DuplicationAddRequest{
		AppName:           tableName,
		RemoteClusterName: remoteCluster,
		Freezed:           freezed,
	}
	err := m.callMeta("AddDuplication", req, func(resp interface{}) {
		result = resp.(*admin.DuplicationAddResponse)
	})
	if result != nil && result.IsSetHint() {
		return result, wrapHintIntoError(*result.Hint, err)
	}
	return result, err
}

func (m *rpcBasedMeta) QueryDuplication(tableName string) (*admin.DuplicationQueryResponse, error) {
	var result *admin.DuplicationQueryResponse
	req := &admin.DuplicationQueryRequest{
		AppName: tableName,
	}
	err := m.callMeta("QueryDuplication", req, func(resp interface{}) {
		result = resp.(*admin.DuplicationQueryResponse)
	})
	return result, err
}

func (m *rpcBasedMeta) ListNodes() ([]*admin.NodeInfo, error) {
	var result []*admin.NodeInfo
	req := &admin.ListNodesRequest{
		Status: admin.NodeStatus_NS_INVALID,
	}
	err := m.callMeta("ListNodes", req, func(resp interface{}) {
		result = resp.(*admin.ListNodesResponse).Infos
	})
	return result, err
}

func (m *rpcBasedMeta) RecallApp(originTableID int, newTableName string) (*admin.AppInfo, error) {
	var result *admin.AppInfo
	req := &admin.RecallAppRequest{
		AppID:       int32(originTableID),
		NewAppName_: newTableName,
	}
	err := m.callMeta("RecallApp", req, func(resp interface{}) {
		result = resp.(*admin.RecallAppResponse).Info
	})
	return result, err
}

func getNodeAddress(n *util.PegasusNode) *base.RPCAddress {
	if n == nil {
		return &base.RPCAddress{}
	}
	return base.NewRPCAddress(n.IP, n.Port)
}

func newProposalAction(target *util.PegasusNode, node *util.PegasusNode, cfgType admin.ConfigType) *admin.ConfigurationProposalAction {
	return &admin.ConfigurationProposalAction{
		Target: getNodeAddress(target),
		Node:   getNodeAddress(node),
		Type:   cfgType,
	}
}

func (m *rpcBasedMeta) Balance(gpid *base.Gpid, opType BalanceType, from *util.PegasusNode, to *util.PegasusNode) error {
	req := &admin.BalanceRequest{
		Gpid: gpid,
	}

	var actions []*admin.ConfigurationProposalAction
	switch opType {
	case BalanceMovePri:
		actions = append(actions, newProposalAction(from, from, admin.ConfigType_CT_DOWNGRADE_TO_SECONDARY))
		actions = append(actions, newProposalAction(to, to, admin.ConfigType_CT_UPGRADE_TO_PRIMARY))
	case BalanceCopyPri:
		actions = append(actions, newProposalAction(from, to, admin.ConfigType_CT_ADD_SECONDARY_FOR_LB))
		actions = append(actions, newProposalAction(to, to, admin.ConfigType_CT_UPGRADE_TO_PRIMARY))
	case BalanceCopySec:
		actions = append(actions, newProposalAction(nil, to, admin.ConfigType_CT_ADD_SECONDARY_FOR_LB))
		actions = append(actions, newProposalAction(nil, from, admin.ConfigType_CT_DOWNGRADE_TO_INACTIVE))
	default:
		return fmt.Errorf("illegal balance type %d", opType)
	}
	req.ActionList = actions

	err := m.callMeta("Balance", req, func(resp interface{}) {})
	return err
}

func (m *rpcBasedMeta) Propose(gpid *base.Gpid, action admin.ConfigType, target *util.PegasusNode, node *util.PegasusNode) error {
	req := &admin.BalanceRequest{
		Gpid: gpid,
		ActionList: []*admin.ConfigurationProposalAction{
			newProposalAction(target, node, action),
		},
	}
	err := m.callMeta("Balance", req, func(resp interface{}) {})
	return err
}
