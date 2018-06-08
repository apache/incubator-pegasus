// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"sync"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/session"
)

type KeyValue struct {
	SortKey, Value []byte
}

type MultiGetOptions struct {
	StartInclusive bool
	StopInclusive  bool
	SortKeyFilter  Filter
	MaxFetchCount  int
	MaxFetchSize   int
}

// Client is the main interface to pegasus.
// Each client is bound to a pegasus cluster specified by `Config`.
// In order to reuse the previous connections, it's recommended to use one singleton
// client in your program. The operations upon a client instance are thread-safe.
type Client interface {
	Close() error

	// Get retrieves the entry for `hashKey` + `sortKey`.
	Get(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) ([]byte, error)

	// Set the entry for `hashKey` + `sortKey` to `value`.
	Set(ctx context.Context, tableName string, hashKey []byte, sortKey []byte, value []byte) error

	// Delete the entry for `hashKey` + `sortKey`.
	Del(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) error

	// MultiGet retrieves the multiple entries under `hashKey` all in one operation.
	// The returned key-value pairs are sorted by sort key in ascending order.
	MultiGet(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte) ([]KeyValue, error)
	MultiGetOpt(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte, options MultiGetOptions) ([]KeyValue, error)

	// MultiGetRange retrieves the multiple entries under `hashKey`, between range (`startSortKey`, `stopSortKey`),
	// all in one operation.
	// The returned key-value pairs are sorted by sort key in ascending order.
	MultiGetRange(ctx context.Context, tableName string, hashKey []byte, startSortKey []byte, stopSortKey []byte) ([]KeyValue, error)
	MultiGetRangeOpt(ctx context.Context, tableName string, hashKey []byte, startSortKey []byte, stopSortKey []byte, options MultiGetOptions) ([]KeyValue, error)

	// MultiSet sets the multiple entries under `hashKey` all in one operation.
	MultiSet(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte, values [][]byte) error
	MultiSetOpt(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte, values [][]byte, ttlSeconds int) error

	// MultiDel deletes the multiple entries under `hashKey` all in one operation.
	// Returns sort key of deleted entries.
	MultiDel(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte) error

	// Open the specific pegasus table. If the table was opened before,
	// it will reuse the previous connection to the table.
	//
	// NOTE: The following two examples have the same effect. But the former is recommended.
	//
	// ```
	//   tb, err := client.OpenTable(context.Background(), "temp")
	//   err = tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1"))
	// ```
	//
	// ```
	//  err := client.Set(context.Background(), "temp", []byte("h1"), []byte("s1"), []byte("v1"))
	// ```
	//
	OpenTable(ctx context.Context, tableName string) (TableConnector, error)

	// Check value existence for the entry for `hashKey` + `sortKey`.
	Exist(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) (bool, error)

	// Get ttl time.
	// ttl: if -1, it means infinity. if -2, it means not found
	TTL(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) (int, error)

	// Get Scanner for {startSortKey, stopSortKey} within hashKey.
	// startSortKey: if null or length == 0, it means start from begin
	// stopSortKey: if null or length == 0, it means stop to end
	GetScanner(ctx context.Context, tableName string, hashKey []byte, startSortKey []byte, stopSortKey []byte, options ScannerOptions) (Scanner, error)

	// Get Scanners for all data in pegasus, the count of scanners will
	// be no more than maxSplitCount
	GetUnorderedScanners(ctx context.Context, tableName string, maxSplitCount int, options ScannerOptions) ([]Scanner, error)
}

type pegasusClient struct {
	tables map[string]TableConnector

	// protect the access of tables
	mu sync.RWMutex

	metaMgr    *session.MetaManager
	replicaMgr *session.ReplicaManager
}

func NewClient(cfg Config) Client {
	if len(cfg.MetaServers) == 0 {
		pegalog.GetLogger().Fatalln("pegasus-go-client: meta sever list should not be empty")
		return nil
	}

	c := &pegasusClient{
		tables:     make(map[string]TableConnector),
		metaMgr:    session.NewMetaManager(cfg.MetaServers, session.NewNodeSession),
		replicaMgr: session.NewReplicaManager(session.NewNodeSession),
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

func (p *pegasusClient) MultiGet(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte) ([]KeyValue, error) {
	return p.MultiGetOpt(ctx, tableName, hashKey, sortKeys, MultiGetOptions{})
}

func (p *pegasusClient) MultiGetOpt(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte, options MultiGetOptions) ([]KeyValue, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return tb.MultiGet(ctx, hashKey, sortKeys, options)
}

func (p *pegasusClient) MultiGetRange(ctx context.Context, tableName string, hashKey []byte, startSortKey []byte, stopSortKey []byte) ([]KeyValue, error) {
	return p.MultiGetRangeOpt(ctx, tableName, hashKey, startSortKey, stopSortKey, MultiGetOptions{})
}

func (p *pegasusClient) MultiGetRangeOpt(ctx context.Context, tableName string, hashKey []byte, startSortKey []byte, stopSortKey []byte, options MultiGetOptions) ([]KeyValue, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return tb.MultiGetRange(ctx, hashKey, startSortKey, stopSortKey, options)
}

func (p *pegasusClient) MultiSet(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte, values [][]byte) error {
	//return nil
	return p.MultiSetOpt(ctx, tableName, hashKey, sortKeys, values, 0)
}

func (p *pegasusClient) MultiSetOpt(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte, values [][]byte, ttlSeconds int) error {
	//return nil
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return err
	}
	return tb.MultiSet(ctx, hashKey, sortKeys, values, ttlSeconds)
}

func (p *pegasusClient) MultiDel(ctx context.Context, tableName string, hashKey []byte, sortKeys [][]byte) error {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return err
	}
	return tb.MultiDel(ctx, hashKey, sortKeys)
}

func (p *pegasusClient) OpenTable(ctx context.Context, tableName string) (TableConnector, error) {
	tb, err := func() (TableConnector, error) {
		// ensure only one goroutine is fetching the routing table.
		p.mu.Lock()
		defer p.mu.Unlock()

		if tb := p.findTable(tableName); tb != nil {
			return tb, nil
		}

		var tb TableConnector
		tb, err := ConnectTable(ctx, tableName, p.metaMgr, p.replicaMgr)
		if err != nil {
			return nil, err
		}
		p.tables[tableName] = tb

		return tb, nil
	}()
	return tb, WrapError(err, OpQueryConfig)
}

func (p *pegasusClient) findTable(tableName string) TableConnector {
	if tb, ok := p.tables[tableName]; ok {
		return tb
	}
	return nil
}

func (p *pegasusClient) Exist(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) (bool, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return false, err
	}
	return tb.Exist(ctx, hashKey, sortKey)
}

func (p *pegasusClient) TTL(ctx context.Context, tableName string, hashKey []byte, sortKey []byte) (int, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return 0, err
	}
	return tb.TTL(ctx, hashKey, sortKey)
}

func (p *pegasusClient) GetScanner(ctx context.Context, tableName string, hashKey []byte, startSortKey []byte, stopSortKey []byte, options ScannerOptions) (Scanner, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return tb.GetScanner(ctx, hashKey, startSortKey, stopSortKey, &options)
}

func (p *pegasusClient) GetUnorderedScanners(ctx context.Context, tableName string, maxSplitCount int, options ScannerOptions) ([]Scanner, error) {
	tb, err := p.OpenTable(ctx, tableName)
	if err != nil {
		return nil, err
	}
	return tb.GetUnorderedScanners(ctx, maxSplitCount, &options)
}
