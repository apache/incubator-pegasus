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

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/rrdb"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
)

// ReplicaSession represents the network session between client and
// replica server.
type ReplicaSession struct {
	NodeSession
}

func (rs *ReplicaSession) Get(ctx context.Context, gpid *base.Gpid, partitionHash uint64, key *base.Blob) (*rrdb.ReadResponse, error) {
	args := &rrdb.RrdbGetArgs{Key: key}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_GET")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbGetResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) Put(ctx context.Context, gpid *base.Gpid, partitionHash uint64, update *rrdb.UpdateRequest) (*rrdb.UpdateResponse, error) {
	args := &rrdb.RrdbPutArgs{Update: update}

	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_PUT")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbPutResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) Del(ctx context.Context, gpid *base.Gpid, partitionHash uint64, key *base.Blob) (*rrdb.UpdateResponse, error) {
	args := &rrdb.RrdbRemoveArgs{Key: key}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_REMOVE")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbRemoveResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) MultiGet(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.MultiGetRequest) (*rrdb.MultiGetResponse, error) {
	args := &rrdb.RrdbMultiGetArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_MULTI_GET")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbMultiGetResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) MultiSet(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.MultiPutRequest) (*rrdb.UpdateResponse, error) {
	args := &rrdb.RrdbMultiPutArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_MULTI_PUT")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbMultiPutResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) MultiDelete(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.MultiRemoveRequest) (*rrdb.MultiRemoveResponse, error) {
	args := &rrdb.RrdbMultiRemoveArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_MULTI_REMOVE")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbMultiRemoveResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) TTL(ctx context.Context, gpid *base.Gpid, partitionHash uint64, key *base.Blob) (*rrdb.TTLResponse, error) {
	args := &rrdb.RrdbTTLArgs{Key: key}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_TTL")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbTTLResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) GetScanner(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.GetScannerRequest) (*rrdb.ScanResponse, error) {
	args := &rrdb.RrdbGetScannerArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_GET_SCANNER")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbGetScannerResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) Scan(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.ScanRequest) (*rrdb.ScanResponse, error) {
	args := &rrdb.RrdbScanArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_SCAN")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbScanResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) ClearScanner(ctx context.Context, gpid *base.Gpid, partitionHash uint64, contextId int64) error {
	args := &rrdb.RrdbClearScannerArgs{ContextID: contextId}
	_, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_CLEAR_SCANNER")
	if err != nil {
		return err
	}

	return nil
}

func (rs *ReplicaSession) CheckAndSet(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.CheckAndSetRequest) (*rrdb.CheckAndSetResponse, error) {
	args := &rrdb.RrdbCheckAndSetArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_CHECK_AND_SET")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbCheckAndSetResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) SortKeyCount(ctx context.Context, gpid *base.Gpid, partitionHash uint64, hashKey *base.Blob) (*rrdb.CountResponse, error) {
	args := &rrdb.RrdbSortkeyCountArgs{HashKey: hashKey}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_SORTKEY_COUNT")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbSortkeyCountResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) Incr(ctx context.Context, gpid *base.Gpid, partitionHash uint64, request *rrdb.IncrRequest) (*rrdb.IncrResponse, error) {
	args := &rrdb.RrdbIncrArgs{Request: request}
	result, err := rs.CallWithGpid(ctx, gpid, partitionHash, args, "RPC_RRDB_RRDB_INCR")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbIncrResult)
	return ret.GetSuccess(), nil
}

// ReplicaManager manages the pool of sessions to replica servers, so that
// different tables that locate on the same replica server can share one
// ReplicaSession, without the effort of creating a new connection.
type ReplicaManager struct {
	//	rpc address -> replica
	replicas map[string]*ReplicaSession
	sync.RWMutex

	creator NodeSessionCreator

	unresponsiveHandler UnresponsiveHandler
}

// UnresponsiveHandler is a callback executed when the session is in unresponsive state.
type UnresponsiveHandler func(NodeSession)

// SetUnresponsiveHandler inits the UnresponsiveHandler.
func (rm *ReplicaManager) SetUnresponsiveHandler(handler UnresponsiveHandler) {
	rm.unresponsiveHandler = handler
}

// Create a new session to the replica server if no existing one.
func (rm *ReplicaManager) GetReplica(addr string) *ReplicaSession {
	rm.Lock()
	defer rm.Unlock()

	if _, ok := rm.replicas[addr]; !ok {
		r := &ReplicaSession{
			NodeSession: rm.creator(addr, NodeTypeReplica),
		}
		withUnresponsiveHandler(r.NodeSession, rm.unresponsiveHandler)
		rm.replicas[addr] = r
	}
	return rm.replicas[addr]
}

func NewReplicaManager(creator NodeSessionCreator) *ReplicaManager {
	return &ReplicaManager{
		replicas: make(map[string]*ReplicaSession),
		creator:  creator,
	}
}

func (rm *ReplicaManager) Close() error {
	rm.Lock()
	defer rm.Unlock()

	funcs := make([]func() error, 0, len(rm.replicas))
	for _, r := range rm.replicas {
		rep := r
		funcs = append(funcs, func() error {
			return rep.Close()
		})
	}
	return kerrors.AggregateGoroutines(funcs...)
}

func (rm *ReplicaManager) ReplicaCount() int {
	rm.RLock()
	defer rm.RUnlock()

	return len(rm.replicas)
}
