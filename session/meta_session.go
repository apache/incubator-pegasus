// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"fmt"
	"sync"

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
	return ms.CallWithGpid(ctx, &base.Gpid{0, 0}, args, rpcName)
}

func (ms *metaSession) queryConfig(ctx context.Context, tableName string) (*replication.QueryCfgResponse, error) {
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

// MetaManager manages the list of metas, but only the leader will it requests to.
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

// QueryConfig queries table configuration from the leader of meta servers. If the leader was changed,
// it retries for other servers until it finds the true leader, unless no leader exists.
// Thread-Safe
func (m *MetaManager) QueryConfig(ctx context.Context, tableName string) (*replication.QueryCfgResponse, error) {
	lead := m.getCurrentLeader()
	meta := m.metas[lead]

	m.logger.Printf("querying configuration of table(%s) from %s [metaList=%s]", tableName, meta, m.metaIPAddrs)
	resp, err := meta.queryConfig(ctx, tableName)

	if ctx.Err() != nil {
		// if the error was due to context death, exit.
		return nil, ctx.Err()
	}
	if err != nil || resp.Err.Errno == base.ERR_FORWARD_TO_OTHERS.String() {
		excluded := lead

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

			m.setCurrentLeader(i)
			return resp, nil
		}

		// when all the responses are ERR_FORWARD_TO_OTHERS
		if err == nil {
			err = fmt.Errorf("unable to find the leader of meta servers")
		}
		return nil, err
	} else {
		return resp, nil
	}
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

func (m *MetaManager) Close() error {
	for _, ns := range m.metas {
		if err := ns.Close(); err != nil {
			return err
		}
	}
	return nil
}
