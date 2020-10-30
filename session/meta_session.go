// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"errors"
	"sync"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
)

// metaSession represents the network session between client and meta server.
type metaSession struct {
	NodeSession

	logger pegalog.Logger
}

func (ms *metaSession) call(ctx context.Context, args RpcRequestArgs, rpcName string) (RpcResponseResult, error) {
	return ms.CallWithGpid(ctx, &base.Gpid{Appid: 0, PartitionIndex: 0}, args, rpcName)
}

func (ms *metaSession) queryConfig(ctx context.Context, tableName string) (*replication.QueryCfgResponse, error) {
	ms.logger.Printf("querying configuration of table(%s) from %s", tableName, ms)

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()
	arg.Query.AppName = tableName
	arg.Query.PartitionIndices = []int32{}

	result, err := ms.call(ctx, arg, "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX")
	if err != nil {
		ms.logger.Printf("failed to query configuration from %s: %s", ms, err)
		return nil, err
	}

	ret, _ := result.(*rrdb.MetaQueryCfgResult)
	return ret.GetSuccess(), nil
}

func (ms *metaSession) createTable(ctx context.Context, tableName string, partitionCount int) (*admin.CreateAppResponse, error) {
	arg := admin.NewAdminClientCreateAppArgs()
	arg.Req = admin.NewCreateAppRequest()
	arg.Req.AppName = tableName
	arg.Req.Options = &admin.CreateAppOptions{
		PartitionCount: int32(partitionCount),
		ReplicaCount:   3,
		AppType:        "pegasus",
		Envs:           make(map[string]string),
		IsStateful:     true,
	}

	result, err := ms.call(ctx, arg, "RPC_CM_CREATE_APP")
	if err != nil {
		ms.logger.Printf("failed to create table from %s: %s", ms, err)
		return nil, err
	}
	ret, _ := result.(*admin.AdminClientCreateAppResult)
	return ret.GetSuccess(), nil
}

func (ms *metaSession) dropTable(ctx context.Context, tableName string) (*admin.DropAppResponse, error) {
	arg := admin.NewAdminClientDropAppArgs()
	arg.Req = admin.NewDropAppRequest()
	arg.Req.AppName = tableName
	reserveSeconds := int64(1) // delete immediately. the caller is responsible for the soft deletion of table.
	arg.Req.Options = &admin.DropAppOptions{
		SuccessIfNotExist: true,
		ReserveSeconds:    &reserveSeconds,
	}

	result, err := ms.call(ctx, arg, "RPC_CM_DROP_APP")
	if err != nil {
		ms.logger.Printf("failed to drop table from %s: %s", ms, err)
		return nil, err
	}
	ret, _ := result.(*admin.AdminClientDropAppResult)
	return ret.GetSuccess(), nil
}

func (ms *metaSession) listTables(ctx context.Context) (*admin.ListAppsResponse, error) {
	arg := admin.NewAdminClientListAppsArgs()
	arg.Req = admin.NewListAppsRequest()
	arg.Req.Status = admin.AppStatus_AS_AVAILABLE

	result, err := ms.call(ctx, arg, "RPC_CM_LIST_APPS")
	if err != nil {
		ms.logger.Printf("failed to list tables from %s: %s", ms, err)
		return nil, err
	}
	ret, _ := result.(*admin.AdminClientListAppsResult)
	return ret.GetSuccess(), nil
}

// MetaManager manages the list of metas, but only the leader will it request to.
// If the one is not the actual leader, it will retry with another.
type MetaManager struct {
	logger pegalog.Logger

	metaIPAddrs   []string
	metas         []*metaSession
	currentLeader int // current leader of meta servers

	// protect access of currentLeader
	mu sync.RWMutex
}

//
func NewMetaManager(addrs []string, creator NodeSessionCreator) *MetaManager {
	metas := make([]*metaSession, len(addrs))
	metaIPAddrs := make([]string, len(addrs))
	for i, addr := range addrs {
		metas[i] = &metaSession{
			NodeSession: creator(addr, NodeTypeMeta),
			logger:      pegalog.GetLogger(),
		}
		metaIPAddrs[i] = addr
	}

	mm := &MetaManager{
		currentLeader: 0,
		metas:         metas,
		metaIPAddrs:   metaIPAddrs,
		logger:        pegalog.GetLogger(),
	}
	return mm
}

func (m *MetaManager) call(ctx context.Context, callFunc metaCallFunc) (metaResponse, error) {
	lead := m.getCurrentLeader()
	call := newMetaCall(lead, m.metas, callFunc)
	resp, err := call.Run(ctx)
	if err == nil {
		m.setCurrentLeader(int(call.newLead))
	}
	return resp, err
}

// QueryConfig queries table configuration from the leader of meta servers. If the leader was changed,
// it retries for other servers until it finds the true leader, unless no leader exists.
// Thread-Safe
func (m *MetaManager) QueryConfig(ctx context.Context, tableName string) (*replication.QueryCfgResponse, error) {
	m.logger.Printf("querying configuration of table(%s) [metaList=%s]", tableName, m.metaIPAddrs)
	resp, err := m.call(ctx, func(rpcCtx context.Context, ms *metaSession) (metaResponse, error) {
		return ms.queryConfig(rpcCtx, tableName)
	})
	if err == nil {
		queryCfgResp := resp.(*replication.QueryCfgResponse)
		return queryCfgResp, nil
	}
	return nil, err
}

// CreateTable creates a table with the specified partition count.
func (m *MetaManager) CreateTable(ctx context.Context, tableName string, partitionCount int) error {
	m.logger.Printf("creating table(%s) with partition count(%d) [metaList=%s]", tableName, partitionCount, m.metaIPAddrs)
	resp, err := m.call(ctx, func(rpcCtx context.Context, ms *metaSession) (metaResponse, error) {
		return ms.createTable(rpcCtx, tableName, partitionCount)
	})
	if err == nil {
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return errors.New(resp.GetErr().String())
		}
		return nil
	}
	return err
}

// DropTable drops a table from the pegasus cluster.
func (m *MetaManager) DropTable(ctx context.Context, tableName string) error {
	m.logger.Printf("dropping table(%s) [metaList=%s]", tableName, m.metaIPAddrs)
	resp, err := m.call(ctx, func(rpcCtx context.Context, ms *metaSession) (metaResponse, error) {
		return ms.dropTable(rpcCtx, tableName)
	})
	if err == nil {
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return errors.New(resp.GetErr().String())
		}
		return nil
	}
	return err
}

// ListTables retrieves all tables' information in the cluster.
func (m *MetaManager) ListTables(ctx context.Context) ([]*admin.AppInfo, error) {
	m.logger.Printf("retrieving the list of tables from [metaList=%s]", m.metaIPAddrs)
	resp, err := m.call(ctx, func(rpcCtx context.Context, ms *metaSession) (metaResponse, error) {
		return ms.listTables(rpcCtx)
	})
	if err == nil {
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return nil, errors.New(resp.GetErr().String())
		}
		listTablesResp := resp.(*admin.ListAppsResponse)
		return listTablesResp.Infos, nil
	}
	return nil, err
}

func (m *MetaManager) getCurrentLeader() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.currentLeader
}

func (m *MetaManager) setCurrentLeader(lead int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentLeader = lead
}

// Close the sessions.
func (m *MetaManager) Close() error {
	for _, ns := range m.metas {
		if err := ns.Close(); err != nil {
			return err
		}
	}
	return nil
}
