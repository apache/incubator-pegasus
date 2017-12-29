// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/pegasus-kv/pegasus-go-client/idl/base"
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

	value, err := tb.Get(context.Background(), []byte("h1"), []byte("s1"))

	_, ok := err.(*PError)
	assert.True(t, ok)

	assert.Equal(t, base.ERR_UNKNOWN, err.(*PError).Code)
	assert.Nil(t, value)
}

// Ensure that table connector will update configuration after at most 10 second.
func TestPegasusTableConnector_AutoUpdate(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second*11)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, _ := client.OpenTable(context.Background(), "temp")
	ptb := tb.(*pegasusTableConnector)

	ptb.mu.Lock()
	ptb.parts = make([]*replicaNode, 0)
	ptb.mu.Unlock()

	time.Sleep(time.Second * 10)
	assert.Equal(t, len(ptb.parts), 8)
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

	ptb.handleError(errors.New("not nil"))
	<-ptb.confUpdateCh

	ptb.handleErrorCode(base.ERR_OBJECT_NOT_FOUND)
	<-ptb.confUpdateCh

	ptb.handleErrorCode(base.ERR_INVALID_STATE)
	<-ptb.confUpdateCh

	// self update can not be triggered by other error codes.
	ptb.handleErrorCode(base.ERR_CLIENT_FAILED)
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
