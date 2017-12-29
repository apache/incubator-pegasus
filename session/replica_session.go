// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"sync"

	"github.com/pegasus-kv/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/pegasus-go-client/idl/rrdb"
)

// ReplicaSession represents the network session between client and
// replica server.
type ReplicaSession struct {
	*nodeSession
}

func newReplicaSession(addr string) *ReplicaSession {
	return &ReplicaSession{
		nodeSession: newNodeSession(addr, kNodeTypeReplica),
	}
}

func (rs *ReplicaSession) Get(ctx context.Context, gpid *base.Gpid, key *base.Blob) (*rrdb.ReadResponse, error) {
	args := &rrdb.RrdbGetArgs{Key: key}
	result, err := rs.callWithGpid(ctx, gpid, args, "RPC_RRDB_RRDB_GET")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbGetResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) Put(ctx context.Context, gpid *base.Gpid, key *base.Blob, value *base.Blob) (*rrdb.UpdateResponse, error) {
	update := &rrdb.UpdateRequest{Key: key, Value: value}
	args := &rrdb.RrdbPutArgs{Update: update}

	result, err := rs.callWithGpid(ctx, gpid, args, "RPC_RRDB_RRDB_PUT")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbPutResult)
	return ret.GetSuccess(), nil
}

func (rs *ReplicaSession) Del(ctx context.Context, gpid *base.Gpid, key *base.Blob) (*rrdb.UpdateResponse, error) {
	args := &rrdb.RrdbRemoveArgs{Key: key}
	result, err := rs.callWithGpid(ctx, gpid, args, "RPC_RRDB_RRDB_REMOVE")
	if err != nil {
		return nil, err
	}

	ret, _ := result.(*rrdb.RrdbRemoveResult)
	return ret.GetSuccess(), nil
}

// ReplicaManager manages the pool of sessions to replica servers, so that
// different tables that locate on the same replica server can share one
// ReplicaSession, without the effort of creating a new connection.
type ReplicaManager struct {
	//	rpc address -> replica
	replicas map[string]*ReplicaSession
	sync.RWMutex
}

// Create a new session to the replica server if no existing one.
func (rm *ReplicaManager) GetReplica(addr string) (rs *ReplicaSession) {
	if rs = rm.findReplica(addr); rs == nil {
		// unable to create connection to the replica
		rm.Lock()
		defer rm.Unlock()
		rs = newReplicaSession(addr)
		rm.replicas[addr] = rs
		return
	}
	return
}

func (rm *ReplicaManager) findReplica(addr string) (rs *ReplicaSession) {
	rm.RLock()
	defer rm.RUnlock()
	if rs, ok := rm.replicas[addr]; ok {
		return rs
	}
	return nil
}

func NewReplicaManager() *ReplicaManager {
	return &ReplicaManager{
		replicas: make(map[string]*ReplicaSession),
	}
}

func (rm *ReplicaManager) Close() error {
	rm.Lock()
	defer rm.Unlock()

	for _, r := range rm.replicas {
		if err := r.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (rm *ReplicaManager) ReplicaCount() int {
	rm.RLock()
	defer rm.RUnlock()

	return len(rm.replicas)
}
