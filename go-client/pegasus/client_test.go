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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestPegasusClient_OpenTable(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb1, err := client.OpenTable(context.Background(), "temp")
	assert.NoError(t, err)
	assert.NotNil(t, tb1)

	tb2, err := client.OpenTable(context.Background(), "temp")
	assert.NoError(t, err)
	assert.NotNil(t, tb1)

	// must reuse previous connection
	assert.Equal(t, tb1, tb2)

	pclient, _ := client.(*pegasusClient)
	assert.NotNil(t, pclient.findTable("temp"))

	tb, err := client.OpenTable(context.Background(), "table_not_exists")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ERR_OBJECT_NOT_FOUND")
	assert.Nil(t, tb)
}

func TestPegasusClient_OpenTableTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	// make sure the port 8801 is not opened on your computer.
	cfg := Config{
		MetaServers: []string{"0.0.0.0:8801"},
	}

	client := NewClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	tb, err := client.OpenTable(ctx, "temp")
	assert.Equal(t, ctx.Err(), context.DeadlineExceeded)
	assert.Nil(t, tb)
	assert.NotNil(t, err)

	client.Close()
}

// Ensure that concurrent OpenTable operations to the same table
// won't invoke more than one query to meta server.
func TestPegasusClient_ConcurrentOpenSameTable(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	openTableQueries := 100
	tblist := make([]TableConnector, openTableQueries)

	var wg sync.WaitGroup
	wg.Add(openTableQueries)
	for i := 0; i < openTableQueries; i++ {
		idx := i
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tb, err := client.OpenTable(ctx, "temp")
			assert.Nil(t, err)
			tblist[idx] = tb
			wg.Done()
		}()
	}
	wg.Wait()

	// all tables returned by OpenTable must be the same one
	tb := tblist[0]
	for i := 1; i < openTableQueries; i++ {
		assert.Equal(t, tb, tblist[i])
	}
}

// In this test we verifies if there's any easy bugs can be found in concurrent rpc.
func TestPegasusClient_ConcurrentMetaQueries(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)

		id := i
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.OpenTable(ctx, "table_not_exist"+fmt.Sprint(id))
			assert.NotNil(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_New(t *testing.T) {
	c, err := newClientWithError(Config{
		MetaServers: []string{"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"},
	})
	assert.Nil(t, err)
	_ = c.Close()

	c, err = newClientWithError(Config{
		MetaServers: []string{"127abc"},
	})
	assert.NotNil(t, err)
	assert.Nil(t, c)

	_, err = newClientWithError(Config{
		MetaServers: []string{},
	})
	assert.NotNil(t, err)
}
