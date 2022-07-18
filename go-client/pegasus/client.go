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

package pegasus

import (
	"context"
	"sync"

	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/apache/incubator-pegasus/go-client/session"
)

// Client manages the client sessions to the pegasus cluster specified by `Config`.
// In order to reuse the previous connections, it's recommended to use one singleton
// client in your program. The operations upon a client instance are thread-safe.
type Client interface {
	Close() error

	// Open the specific pegasus table. If the table was opened before,
	// it will reuse the previous connection to the table.
	OpenTable(ctx context.Context, tableName string) (TableConnector, error)
}

type pegasusClient struct {
	tables map[string]TableConnector

	// protect the access of tables
	mu sync.RWMutex

	metaMgr    *session.MetaManager
	replicaMgr *session.ReplicaManager
}

// NewClient creates a new instance of pegasus client.
// It panics if the configured addresses are illegal.
func NewClient(cfg Config) Client {
	c, err := newClientWithError(cfg)
	if err != nil {
		pegalog.GetLogger().Fatal(err)
		return nil
	}
	return c
}

func newClientWithError(cfg Config) (Client, error) {
	var err error
	cfg.MetaServers, err = session.ResolveMetaAddr(cfg.MetaServers)
	if err != nil {
		return nil, err
	}

	c := &pegasusClient{
		tables:     make(map[string]TableConnector),
		metaMgr:    session.NewMetaManager(cfg.MetaServers, session.NewNodeSession),
		replicaMgr: session.NewReplicaManager(session.NewNodeSession),
	}
	return c, nil
}

func (p *pegasusClient) Close() error {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for _, table := range p.tables {
		if err := table.Close(); err != nil {
			return err
		}
	}

	if err := p.metaMgr.Close(); err != nil {
		pegalog.GetLogger().Fatal("pegasus-go-client: unable to close MetaManager: ", err)
	}
	return p.replicaMgr.Close()
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
