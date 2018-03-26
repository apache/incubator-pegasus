// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

// This is the integration test of the client. Please start the pegasus onebox
// before you running the tests.

func TestPegasusTableConnector_Get(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, _ := client.OpenTable(context.Background(), "temp")

	assert.Nil(t, tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1")))

	value, err := tb.Get(context.Background(), []byte("h1"), []byte("s1"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("v1"), value)
}

func TestPegasusTableConnector_Del(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, _ := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, tb.Del(context.Background(), []byte("h1"), []byte("s1")))

	value, _ := tb.Get(context.Background(), []byte("h1"), []byte("s1"))
	assert.Empty(t, value)
}

func TestPegasusTableConnector_TriggerSelfUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	ptb, _ := tb.(*pegasusTableConnector)

	err = ptb.handleError(nil, nil, nil)
	assert.Nil(t, err)

	ptb.handleError(errors.New("not nil"), nil, nil)
	<-ptb.confUpdateCh

	ptb.handleError(base.ERR_OBJECT_NOT_FOUND, nil, nil)
	<-ptb.confUpdateCh

	ptb.handleError(base.ERR_INVALID_STATE, nil, nil)
	<-ptb.confUpdateCh

	// self update can not be triggered by other error codes.
	ptb.handleError(base.ERR_CLIENT_FAILED, nil, nil)
	select {
	case <-ptb.confUpdateCh:
	default:
		assert.True(t, true)
	}
}

func TestPegasusTableConnector_SubsequentSelfUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	ptb, _ := tb.(*pegasusTableConnector)

	count := 0
	for i := 0; i < 10; i++ {
		if ptb.selfUpdate() {
			count++
		}
	}
	assert.Equal(t, count, 0)
}

func TestPegasusTableConnector_ValidateHashKey(t *testing.T) {
	var hashKey []byte

	hashKey = nil
	assert.NotNil(t, validateHashKey(hashKey))

	hashKey = make([]byte, 0)
	assert.NotNil(t, validateHashKey(hashKey))

	hashKey = make([]byte, math.MaxUint16+1)
	assert.NotNil(t, validateHashKey(hashKey))
}
