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
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/rpc"
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

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)

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

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)

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

	err = ptb.handleReplicaError(nil, nil, nil)
	assert.Nil(t, err)

	ptb.handleReplicaError(errors.New("not nil"), nil, nil)
	<-ptb.confUpdateCh

	ptb.handleReplicaError(base.ERR_OBJECT_NOT_FOUND, nil, nil)
	<-ptb.confUpdateCh

	ptb.handleReplicaError(base.ERR_INVALID_STATE, nil, nil)
	<-ptb.confUpdateCh

	{ // Ensure: The following errors should not trigger configuration update
		errorTypes := []error{base.ERR_TIMEOUT, context.DeadlineExceeded, base.ERR_CAPACITY_EXCEEDED, base.ERR_NOT_ENOUGH_MEMBER}

		for _, err := range errorTypes {
			channelEmpty := false
			ptb.handleReplicaError(err, nil, nil)
			select {
			case <-ptb.confUpdateCh:
			default:
				channelEmpty = true
			}
			assert.True(t, channelEmpty)
		}
	}
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

func TestPegasusTableConnector_HandleInvalidQueryConfigResp(t *testing.T) {
	defer leaktest.Check(t)()

	p := &pegasusTableConnector{
		tableName: "temp",
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_BUSY"}

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
		assert.Equal(t, err.Error(), "ERR_BUSY")
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_OK"}

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)

		resp.Partitions = make([]*replication.PartitionConfiguration, 10)
		resp.PartitionCount = 5
		err = p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_OK"}

		resp.Partitions = make([]*replication.PartitionConfiguration, 4)
		resp.PartitionCount = 4

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
		assert.Equal(t, len(p.parts), 4)
	}
}

func TestPegasusTableConnector_Close(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	// Ensure: Closing table doesn't close the connections.

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	ptb, _ := tb.(*pegasusTableConnector)

	err = tb.Set(context.Background(), []byte("a"), []byte("a"), []byte("a"))
	assert.Nil(t, err)

	ptb.Close()
	_, r := ptb.getPartition([]byte("a"))
	assert.Equal(t, r.ConnState(), rpc.ConnStateReady)
}
