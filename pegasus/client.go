// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"sync"

	"github.com/pegasus-kv/pegasus-go-client/session"
)

// Client is the main interface to pegasus.
type Client interface {
	Close() error

	Get(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) ([]byte, error)

	Set(ctx context.Context, tableName string, hashKey []byte, sortKey []byte, value []byte) error

	Del(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) error

	OpenTable(ctx context.Context, tableName string) (TableConnector, error)
}

type pegasusClient struct {
	tables map[string]TableConnector

	// protect the access of tables
	mu sync.RWMutex

	metaMgr    *session.MetaManager
	replicaMgr *session.ReplicaManager
}

func NewClient(cfg Config) Client {
	c := &pegasusClient{
		tables:     make(map[string]TableConnector),
		metaMgr:    session.NewMetaManager(cfg.MetaServers),
		replicaMgr: session.NewReplicaManager(),
	}
	return c
}

func (p *pegasusClient) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, table := range p.tables {
		if err := table.Close(); err != nil {
			return err
		}
	}

	err := p.metaMgr.Close()
	err = p.replicaMgr.Close()
	return err
}

func (p *pegasusClient) Get(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) ([]byte, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return tb.Get(ctx, hashKey, sortKey)
}

func (p *pegasusClient) Set(ctx context.Context, tableName string, hashKey []byte, sortKey []byte, value []byte) error {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return err
	}
	return tb.Set(ctx, hashKey, sortKey, value)
}

func (p *pegasusClient) Del(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) error {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return err
	}
	return tb.Del(ctx, hashKey, sortKey)
}

// Open the specific pegasus table. If the table was opened before,
// it will reuse the previous connection to the table.
func (p *pegasusClient) OpenTable(ctx context.Context, tableName string) (TableConnector, error) {
	tb, err := func() (TableConnector, error) {
		// ensure there's only one goroutine trying to connect this table.
		p.mu.Lock()
		defer p.mu.Unlock()

		if tb := p.findTable(tableName); tb != nil {
			return tb, nil
		}

		var tb TableConnector
		tb, err := connectTable(ctx, tableName, p.metaMgr, p.replicaMgr)
		if err != nil {
			return nil, err
		}
		p.tables[tableName] = tb

		return tb, nil
	}()
	return tb, wrapError(err, OpQueryConfig)
}

func (p *pegasusClient) findTable(tableName string) TableConnector {
	if tb, ok := p.tables[tableName]; ok {
		return tb
	}
	return nil
}
