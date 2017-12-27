// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package rpc

import (
	"gopkg.in/tomb.v2"
	"sync"
)

type threadSafeConnMap struct {
	connMap map[string]*RpcConn
	mu      sync.Mutex
}

var globalConnMap = &threadSafeConnMap{connMap: make(map[string]*RpcConn)}

func (cm *threadSafeConnMap) findOrCreateConn(parent *tomb.Tomb, addr string) *RpcConn {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if conn, ok := cm.connMap[addr]; ok {
		return conn
	}

	conn := NewRpcConn(parent, addr)
	cm.connMap[addr] = conn
	return conn
}

func (cm *threadSafeConnMap) removeConn(conn *RpcConn) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	delete(cm.connMap, conn.Endpoint)
	return
}

// FindOrCreateConn create a new RpcConn if a connection for addr
// doesn't already exist.
// The returned value is always non-nil even if the connection attempt
// was failed.
// If the connection is newly created, its lifetime will be bounded with
// the provided parent tomb. When the parent routine is killed,
// the RpcConn instance will also be closed.
// This function is thread-safe.
func FindOrCreateConn(parent *tomb.Tomb, addr string) (*RpcConn, error) {
	conn := globalConnMap.findOrCreateConn(parent, addr)

	// TODO(wutao1): retry after connect failure.
	if err := conn.dial(); err != nil {
		return conn, err
	} else {
		return conn, nil
	}
}

// Close the specified connection and remove it from globalConnMap.
// This function is thread-safe.
func CloseConn(conn *RpcConn) error {
	globalConnMap.removeConn(conn)
	err := conn.Close()
	conn = nil
	return err
}
