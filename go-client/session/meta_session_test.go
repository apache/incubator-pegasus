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
	"testing"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

// ensure context.cancel is able to interrupt the RPC.
func TestNodeSession_ContextCancel(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mm := NewMetaManager([]string{"0.0.0.0:34601"}, NewNodeSession)
	defer mm.Close()
	_, err := mm.QueryConfig(ctx, "temp")

	assert.Equal(t, err, ctx.Err())
}

func TestNodeSession_Call(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:34601")
	defer meta.Close()

	_, err := meta.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
}

func TestMetaSession_MustQueryLeader(t *testing.T) {
	testMetaSessionMustQueryLeader(t, []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"})
	testMetaSessionMustQueryLeader(t, []string{"0.0.0.0:12345", "0.0.0.0:12346", "0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"})
}

func testMetaSessionMustQueryLeader(t *testing.T, metaServers []string) {
	defer leaktest.Check(t)()

	mm := NewMetaManager(metaServers, NewNodeSession)
	defer mm.Close()

	resp, err := mm.QueryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())

	// the cached leader must be the actual leader
	ms := mm.metas[mm.currentLeader]
	ms.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())
}

// Ensure that concurrent query_config calls won't make errors.
func TestNodeSession_ConcurrentCall(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:34601")
	defer meta.Close()

	var wg sync.WaitGroup
	wg.Add(5)
	for i := 0; i < 5; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			_, err := meta.queryConfig(ctx, "temp")
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()
}

// This test mocks the case that the first meta is unavailable. The MetaManager must be able to try
// communicating with the other metas.
func TestMetaManager_FirstMetaDead(t *testing.T) {
	defer leaktest.Check(t)()

	// the first meta is invalid
	mm := NewMetaManager([]string{"0.0.0.0:12345", "0.0.0.0:34603", "0.0.0.0:34602", "0.0.0.0:34601"}, NewNodeSession)
	defer mm.Close()

	queryStart := time.Now()
	resp, err := mm.QueryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())
	// The time duration must larger than 1s. Because it needs at least 1s to fallback to the backup metas.
	assert.Greater(t, int64(time.Since(queryStart)), int64(time.Second))

	// ensure the subsequent queries issue only to the leader
	for i := 0; i < 3; i++ {
		call := newMetaCall(mm.currentLeader, mm.metas, func(rpcCtx context.Context, ms *metaSession) (metaResponse, error) {
			return ms.queryConfig(rpcCtx, "temp")
		}, []string{"0.0.0.0:12345", "0.0.0.0:34603", "0.0.0.0:34602", "0.0.0.0:34601"})
		// This a trick for testing. If metaCall issue to other meta, not only to the leader, this nil channel will cause panic.
		call.backupCh = nil
		metaResp, err := call.Run(context.Background())
		assert.Nil(t, err)
		assert.Equal(t, metaResp.GetErr().Errno, base.ERR_OK.String())
	}
}

// This case mocks the case that the server primary meta is not in the client metalist.
// And the client will forward to the primary meta automatically.
func TestNodeSession_ForwardToPrimaryMeta(t *testing.T) {
	defer leaktest.Check(t)()

	metaList := []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"}

	for i := 0; i < 3; i++ {
		mm := NewMetaManager(metaList[i:i+1], NewNodeSession)
		defer mm.Close()
		resp, err := mm.QueryConfig(context.Background(), "temp")
		assert.Nil(t, err)
		assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())
	}
}
