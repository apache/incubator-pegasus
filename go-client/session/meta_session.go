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

package session

import (
	"context"
	"net"
	"strings"
	"sync"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/idl/rrdb"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
)

// metaSession represents the network session between client and meta server.
type metaSession struct {
	NodeSession

	logger pegalog.Logger
}

func (ms *metaSession) call(ctx context.Context, args RpcRequestArgs, rpcName string) (RpcResponseResult, error) {
	return ms.CallWithGpid(ctx, &base.Gpid{Appid: 0, PartitionIndex: 0}, 0, args, rpcName)
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

// MetaManager manages the list of metas, but only the leader will it request to.
// If the one is not the actual leader, it will retry with another.
type MetaManager struct {
	logger pegalog.Logger

	metaIPAddrs   []string
	metaDomain    string
	metas         []*metaSession
	currentLeader int // current leader of meta servers

	// protect access of currentLeader
	mu sync.RWMutex
}

func NewMetaManager(addrs []string, creator NodeSessionCreator) *MetaManager {
	metaHostAddr := ""
	if len(addrs) == 1 && isDomain(addrs[0]) {
		metaHostAddr = addrs[0]
	}

	addrs, err := ResolveMetaAddr(addrs)
	if err != nil {
		return nil
	}

	metas := make([]*metaSession, len(addrs))
	metaIPAddrs := make([]string, len(addrs))
	for i, addr := range addrs {
		metas[i] = &metaSession{
			NodeSession: creator(addr, NodeTypeMeta, DisableMetrics),
			logger:      pegalog.GetLogger(),
		}
		metaIPAddrs[i] = addr
	}

	mm := &MetaManager{
		currentLeader: 0,
		metas:         metas,
		metaIPAddrs:   metaIPAddrs,
		metaDomain:    metaHostAddr,
		logger:        pegalog.GetLogger(),
	}
	return mm
}

func isDomain(s string) bool {
	host := s
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		host = host[:idx]
	}

	if ip := net.ParseIP(host); ip != nil {
		return false
	}

	ips, err := net.LookupIP(host)
	if err != nil || len(ips) == 0 {
		return false
	}

	return true
}

func (m *MetaManager) call(ctx context.Context, callFunc metaCallFunc) (metaResponse, error) {
	call := newMetaCall(m.getCurrentLeader(), m.metas, callFunc, m.metaIPAddrs)
	resp, err := call.Run(ctx)
	if err == nil {
		call.lock.RLock()
		m.setCurrentLeader(int(call.newLead))
		m.setNewMetas(call.metas)
		m.setMetaIPAddrs(call.metaIPAddrs)
		call.lock.RUnlock()
	} else if m.metaDomain != "" {
		if newAddrs, err := ResolveMetaAddr([]string{m.metaDomain}); err == nil {
			m.mu.Lock()
			m.logger.Printf("resolved meta list %s to %v", m.metaDomain, newAddrs)
			anyNew := false
			for _, addr := range newAddrs {
				if addMetaSession(&m.metaIPAddrs, &m.metas, addr) {
					anyNew = true
				}
			}
			if anyNew {
				m.currentLeader = len(m.metas) - 1
			}
			m.mu.Unlock()

		}
	}
	return resp, err
}

func addMetaSession(metaIPAddrs *[]string, metas *[]*metaSession, addr string) bool {
	found := false
	for _, oldAddr := range *metaIPAddrs {
		if oldAddr == addr {
			found = true
			break
		}
	}

	if !found {
		*metaIPAddrs = append(*metaIPAddrs, addr)
		*metas = append(*metas, &metaSession{
			NodeSession: newNodeSession(addr, NodeTypeMeta, DisableMetrics),
			logger:      pegalog.GetLogger(),
		})
		return true
	}

	return false
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

func (m *MetaManager) GetMetaIPAddrs() []string {
	return m.metaIPAddrs
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

func (m *MetaManager) setNewMetas(metas []*metaSession) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.metas = metas
}

func (m *MetaManager) setMetaIPAddrs(metaIPAddrs []string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.metaIPAddrs = metaIPAddrs
}

// Close the sessions.
func (m *MetaManager) Close() error {
	funcs := make([]func() error, len(m.metas))
	for i := 0; i < len(m.metas); i++ {
		idx := i
		funcs[idx] = func() error {
			return m.metas[idx].Close()
		}
	}
	return kerrors.AggregateGoroutines(funcs...)
}
