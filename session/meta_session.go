// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"sync"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
)

// MetaManager manages the list of metas, but only the leader will it requests to.
// If the one is not the actual leader, it will retry with another.
type MetaManager struct {
	logger pegalog.Logger

	metas         []*metaSession
	currentLeader int // current leader of meta servers

	// protect access of currentLeader
	mu sync.RWMutex
}

// metaSession represents the network session between client and meta server.
type metaSession struct {
	*nodeSession
}

func newMetaSession(addr string) *metaSession {
	return &metaSession{
		nodeSession: newNodeSession(addr, kNodeTypeMeta),
	}
}

func (ms *metaSession) call(ctx context.Context, args rpcRequestArgs, rpcName string) (rpcResponseResult, error) {
	return ms.callWithGpid(ctx, &base.Gpid{0, 0}, args, rpcName)
}

func (ms *metaSession) queryConfig(ctx context.Context, tableName string) (*replication.QueryCfgResponse, error) {
	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()
	arg.Query.AppName = tableName
	arg.Query.PartitionIndices = []int32{}

	ms.logger.Printf("querying configuration of table(%s) from [%s, %s]", tableName, ms.addr, ms.ntype)
	result, err := ms.call(ctx, arg, "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX")
	if err != nil {
		ms.logger.Printf("failed to query configuration from meta %s: %s", ms.addr, err)
		return nil, err
	}

	ret, _ := result.(*rrdb.MetaQueryCfgResult)
	return ret.GetSuccess(), nil
}

//
func NewMetaManager(addrs []string) *MetaManager {
	metas := make([]*metaSession, len(addrs))
	for i, addr := range addrs {
		metas[i] = newMetaSession(addr)
	}

	mm := &MetaManager{
		currentLeader: 0,
		metas:         metas,
		logger:        pegalog.GetLogger(),
	}
	return mm
}

func (m *MetaManager) QueryConfig(ctx context.Context, tableName string) (*replication.QueryCfgResponse, error) {
	meta := m.metas[m.currentLeader]
	resp, err := meta.queryConfig(ctx, tableName)

	if ctx.Err() != nil {
		// if the error was due to context death, exit.
		return nil, ctx.Err()
	}
	if err != nil || resp.Err.Errno == base.ERR_FORWARD_TO_OTHERS.String() {
		excluded := m.currentLeader

		// try other nodes, if finally we are unable to find any node that's
		// available, we will give up and return error.
		for i, meta := range m.metas {
			if i == excluded {
				continue
			}

			resp, err = meta.queryConfig(ctx, tableName)
			if ctx.Err() != nil {
				// exit if the context was cancelled
				return nil, ctx.Err()
			}
			if err != nil || resp.Err.Errno == base.ERR_FORWARD_TO_OTHERS.String() {
				continue
			}

			m.currentLeader = i
			return resp, nil
		}
		return nil, err
	} else {
		return resp, nil
	}
}

func (m *MetaManager) Close() error {
	for _, ns := range m.metas {
		<-ns.Close()
	}
	return nil
}
