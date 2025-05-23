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
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
)

type metaCallFunc func(context.Context, *metaSession) (metaResponse, error)

type metaResponse interface {
	GetErr() *base.ErrorCode
}

// metaCall encapsulates the leader switching of MetaServers. Each metaCall.Run represents
// a RPC call to the MetaServers. If during the process the leader meta changed, metaCall
// automatically switches to the new leader.
type metaCall struct {
	respCh   chan metaResponse
	backupCh chan interface{}
	callFunc metaCallFunc

	metaIPAddrs []string
	metas       []*metaSession
	lead        int
	// After a Run successfully ends, the current leader will be set in this field.
	// If there is no meta failover, `newLead` equals to `lead`.
	newLead uint32
	lock    sync.RWMutex
}

func newMetaCall(lead int, metas []*metaSession, callFunc metaCallFunc, meatIPAddr []string) *metaCall {
	return &metaCall{
		metas:       metas,
		metaIPAddrs: meatIPAddr,
		lead:        lead,
		newLead:     uint32(lead),
		respCh:      make(chan metaResponse),
		callFunc:    callFunc,
		backupCh:    make(chan interface{}),
	}
}

func (c *metaCall) Run(ctx context.Context) (metaResponse, error) {
	// the subroutines will be cancelled when this call ends
	subCtx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(2) // this waitgroup is used to ensure all goroutines exit after Run ends.

	go func() {
		defer wg.Done()

		// issue RPC to leader
		if !c.issueSingleMeta(subCtx, c.lead) {
			select {
			case <-subCtx.Done():
			case c.backupCh <- nil:
				// after the leader failed, we immediately start another
				// RPC to the backup.
			}
		}
	}()

	go func() {
		defer wg.Done()

		// Automatically issue backup RPC after a period
		// when the current leader is suspected unvailable.
		select {
		case <-time.After(1 * time.Second): // TODO(wutao): make it configurable
			c.issueBackupMetas(subCtx)
		case <-c.backupCh:
			c.issueBackupMetas(subCtx)
		case <-subCtx.Done():
		}
	}()

	// The result of meta query is always a context error, or success.
	select {
	case resp := <-c.respCh:
		cancel()
		wg.Wait()
		return resp, nil
	case <-ctx.Done():
		cancel()
		wg.Wait()
		return nil, ctx.Err()
	}
}

// issueSingleMeta returns false if we should try another meta
func (c *metaCall) issueSingleMeta(ctx context.Context, curLeader int) bool {
	c.lock.RLock()
	meta := c.metas[curLeader]
	c.lock.RUnlock()

	resp, err := c.callFunc(ctx, meta)
	if err == nil && resp.GetErr().Errno == base.ERR_FORWARD_TO_OTHERS.String() {
		forwardAddr := getMetaServiceForwardAddress(resp)
		if forwardAddr == nil {
			return false
		}
		addr := forwardAddr.GetAddress()
		found := false
		c.lock.Lock()
		for i := range c.metaIPAddrs {
			if addr == c.metaIPAddrs[i] {
				found = true
				break
			}
		}
		c.lock.Unlock()
		if !found {
			c.lock.Lock()
			c.metaIPAddrs = append(c.metaIPAddrs, addr)
			c.metas = append(c.metas, &metaSession{
				NodeSession: newNodeSession(addr, NodeTypeMeta),
				logger:      pegalog.GetLogger(),
			})
			curLeader = len(c.metas) - 1
			c.metas[curLeader].logger.Printf("add forward address %s as meta server", addr)
			meta = c.metas[curLeader]
			c.lock.Unlock()

			resp, err = c.callFunc(ctx, meta)
		}
	}

	if err != nil || resp.GetErr().Errno == base.ERR_FORWARD_TO_OTHERS.String() {
		return false
	}
	// the RPC succeeds, this meta becomes the new leader now.
	atomic.StoreUint32(&c.newLead, uint32(curLeader))
	select {
	case <-ctx.Done():
	case c.respCh <- resp:
		// notify the caller
	}
	return true
}

func (c *metaCall) issueBackupMetas(ctx context.Context) {
	c.lock.RLock()
	metas := append([]*metaSession(nil), c.metas...)
	lead := c.lead
	c.lock.RUnlock()

	var wg sync.WaitGroup
	for i := range metas {
		wg.Add(1)

		if i == lead {
			continue
		}

		// concurrently issue RPC to the rest of meta servers.
		go func(idx int) {
			defer wg.Done()
			c.issueSingleMeta(ctx, idx)
		}(i)
	}

	wg.Wait()
}

func getMetaServiceForwardAddress(resp metaResponse) *base.RPCAddress {
	queryCfgResp, ok := resp.(*replication.QueryCfgResponse)
	if !ok || queryCfgResp.GetErr().Errno != base.ERR_FORWARD_TO_OTHERS.String() {
		return nil
	}

	if queryCfgResp.GetPartitions() == nil || len(queryCfgResp.GetPartitions()) == 0 {
		return nil
	}

	// The forward address will be put in partitions[0].primary if exist.
	// See query_cfg_response's definition in idl/dsn.layer2.thrift and
	// meta_service::on_query_configuration_by_index
	return queryCfgResp.Partitions[0].Primary
}
