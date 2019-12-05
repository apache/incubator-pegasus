// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"sync"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
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
	defer leaktest.Check(t)()

	mm := NewMetaManager([]string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"}, NewNodeSession)
	defer mm.Close()

	resp, err := mm.QueryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())

	// the viewed leader must be the actual leader
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
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			_, err := meta.queryConfig(context.Background(), "temp")
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()
}
