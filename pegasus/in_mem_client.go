// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"

	"github.com/pegasus-kv/pegasus-go-client/pegalog"
)

// Create a fake client to pegasus which keeps everything in memory.
func NewClientInMemory() Client {
	return &inMemClient{
		tables: make(map[string]*inMemTableConnector),
		logger: pegalog.GetLogger(),
	}
}

type inMemClient struct {
	tables map[string]*inMemTableConnector

	logger pegalog.Logger
}

func (p *inMemClient) Get(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) ([]byte, error) {
	tb, _ := p.OpenTable(ctx, tableName)
	return tb.Get(ctx, hashKey, sortKey)
}

func (p *inMemClient) Set(ctx context.Context, tableName string, hashKey []byte, sortKey []byte, value []byte) error {
	tb, _ := p.OpenTable(ctx, tableName)
	return tb.Set(ctx, hashKey, sortKey, value)
}

func (p *inMemClient) Del(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) error {
	tb, _ := p.OpenTable(ctx, tableName)
	return tb.Del(ctx, hashKey, sortKey)
}

func (p *inMemClient) OpenTable(ctx context.Context, tableName string) (TableConnector, error) {
	tb, ok := p.tables[tableName]
	if !ok {
		tb = &inMemTableConnector{
			logger: pegalog.GetLogger(),
			hashes: make(map[string]partition),
		}
		p.tables[tableName] = tb
	}
	return tb, nil
}

func (p *inMemClient) Close() error {
	return nil
}

// Go doesn't support customized struct as map key, so we have to
// convert byte array into string.

type partition map[string]string

type inMemTableConnector struct {
	hashes map[string]partition
	logger pegalog.Logger
}

func (p *inMemTableConnector) Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error) {
	part, ok := p.hashes[string(hashKey)]
	if ok {
		var value string
		value, ok = part[string(sortKey)]
		if ok {
			return []byte(value), nil
		}
	}
	return nil, nil
}

func (p *inMemTableConnector) Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error {
	part, ok := p.hashes[string(hashKey)]
	if !ok {
		part = make(partition)
		p.hashes[string(hashKey)] = part
	}
	part[string(sortKey)] = string(value)
	return nil
}

func (p *inMemTableConnector) Del(ctx context.Context, hashKey []byte, sortKey []byte) error {
	part, ok := p.hashes[string(hashKey)]
	if ok {
		delete(part, string(sortKey))
	}
	return nil
}

func (p *inMemTableConnector) Close() error {
	return nil
}
