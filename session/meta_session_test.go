// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"testing"

	"github.com/fortytw2/leaktest"
	"github.com/pegasus-kv/pegasus-go-client/idl/base"
	"github.com/stretchr/testify/assert"
)

func TestNodeSession_ContextCancel(t *testing.T) {
	defer leaktest.Check(t)()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	mm := NewMetaManager([]string{"0.0.0.0:34601"})
	defer mm.Close()
	_, err := mm.QueryConfig(ctx, "temp")

	assert.Equal(t, err, ctx.Err())
}

func TestNodeSession_Call(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:34601")
	defer meta.Close()

	resp, err := meta.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	assert.Equal(t, resp.Err.Errno, base.ERR_OK.String())
}

func TestMetaSession_MustQueryLeader(t *testing.T) {
	defer leaktest.Check(t)()

	mm := NewMetaManager([]string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"})
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
