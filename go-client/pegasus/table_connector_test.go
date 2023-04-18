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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/apache/incubator-pegasus/go-client/rpc"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

// This is the integration test of the client. Please start the pegasus onebox
// before you running the tests.

func testSingleKeyOperations(t *testing.T, tb TableConnector, hashKey []byte, sortKey []byte, value []byte) {
	// read after write
	assert.Nil(t, tb.Del(context.Background(), hashKey, sortKey))
	assert.Nil(t, tb.Set(context.Background(), hashKey, sortKey, value))
	result, err := tb.Get(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, value, result)
	exist, err := tb.Exist(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, true, exist)

	// ensure GET a non-existed entry returns a nil value
	assert.Nil(t, tb.Del(context.Background(), hashKey, sortKey))
	result = nil
	result, err = tb.Get(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Nil(t, result)
	exist, err = tb.Exist(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, false, exist)

	// === ttl === //

	ttl, err := tb.TTL(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, -2)

	assert.Nil(t, tb.Set(context.Background(), hashKey, sortKey, value))
	ttl, err = tb.TTL(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Equal(t, ttl, -1)

	assert.Nil(t, tb.SetTTL(context.Background(), hashKey, sortKey, value, time.Second*10))
	ttl, err = tb.TTL(context.Background(), hashKey, sortKey)
	assert.Nil(t, err)
	assert.Condition(t, func() bool {
		// pegasus server may return a ttl slightly different
		// from the value we set.
		return ttl <= 11 && ttl >= 9
	})
	// test with invalid ttl
	assert.Error(t, tb.SetTTL(context.Background(), hashKey, sortKey, value, -10))

	assert.Nil(t, tb.Del(context.Background(), hashKey, sortKey))
}

var testingCfg = Config{
	MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
}

func TestPegasusTableConnector_SingleKeyOperations(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	// run sequentially
	for i := 0; i < 10; i++ {
		hashKey := []byte(fmt.Sprintf("h%d", i))
		sortKey := []byte(fmt.Sprintf("s%d", i))
		value := []byte(fmt.Sprintf("v%d", i))
		testSingleKeyOperations(t, tb, hashKey, sortKey, value)
	}

	// run concurrently
	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		id := i
		go func() {
			hashKey := []byte(fmt.Sprintf("h%d", id))
			sortKey := []byte(fmt.Sprintf("s%d", id))
			value := []byte(fmt.Sprintf("v%d", id))

			testSingleKeyOperations(t, tb, hashKey, sortKey, value)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusTableConnector_EmptyInput(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	// Get
	_, err = tb.Get(context.Background(), nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	_, err = tb.Get(context.Background(), []byte{}, nil)
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	_, err = tb.Get(context.Background(), []byte("h1"), nil)
	assert.Contains(t, err.Error(), "sortkey must not be nil")
	_, err = tb.Get(context.Background(), []byte("h1"), []byte(""))
	assert.Nil(t, err)

	// Set
	err = tb.SetTTL(context.Background(), nil, nil, nil, 0)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	err = tb.Set(context.Background(), []byte{}, nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	err = tb.SetTTL(context.Background(), []byte("h1"), nil, []byte(""), 0)
	assert.Contains(t, err.Error(), "sortkey must not be nil")
	err = tb.SetTTL(context.Background(), []byte("h1"), []byte(""), nil, 0)
	assert.Contains(t, err.Error(), "value must not be nil")
	err = tb.SetTTL(context.Background(), []byte("h1"), []byte(""), []byte(""), 0)
	assert.Nil(t, err)

	// Del
	err = tb.Del(context.Background(), nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	err = tb.Del(context.Background(), []byte{}, nil)
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	err = tb.Del(context.Background(), []byte("h1"), nil)
	assert.Contains(t, err.Error(), "sortkey must not be nil")
	err = tb.Del(context.Background(), []byte("h1"), []byte(""))
	assert.Nil(t, err)

	// MultiGet
	_, _, err = tb.MultiGet(context.Background(), nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	_, _, err = tb.MultiGetOpt(context.Background(), []byte{}, nil, &MultiGetOptions{})
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	_, _, err = tb.MultiGet(context.Background(), []byte("h1"), nil)
	assert.Nil(t, err)
	_, _, err = tb.MultiGetOpt(context.Background(), []byte("h1"), [][]byte{}, &MultiGetOptions{})
	assert.Nil(t, err)
	_, _, err = tb.MultiGetOpt(context.Background(), []byte("h1"), [][]byte{nil}, &MultiGetOptions{})
	assert.Contains(t, err.Error(), "sortkeys[0] must not be nil")

	// MultiGetRange
	_, _, err = tb.MultiGetRange(context.Background(), nil, nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	_, _, err = tb.MultiGetRangeOpt(context.Background(), []byte{}, nil, nil, &MultiGetOptions{})
	assert.Contains(t, err.Error(), "hashkey must not be empty")

	// MultiSet
	err = tb.MultiSet(context.Background(), nil, nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	err = tb.MultiSetOpt(context.Background(), []byte{}, nil, nil, 0)
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	err = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{[]byte("s1")}, nil, 0)
	assert.Contains(t, err.Error(), "values must not be nil")
	err = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{[]byte("s1")}, [][]byte{}, 0)
	assert.Contains(t, err.Error(), "values must not be empty")
	err = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{[]byte("s1")}, [][]byte{nil}, 0)
	assert.Contains(t, err.Error(), "values[0] must not be nil")
	err = tb.MultiSet(context.Background(), []byte("h1"), nil, [][]byte{[]byte("v1")})
	assert.Contains(t, err.Error(), "sortkeys must not be nil")
	err = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{}, [][]byte{[]byte("v1")}, 0)
	assert.Contains(t, err.Error(), "sortkeys must not be empty")
	err = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{nil}, [][]byte{[]byte("v1")}, 0)
	assert.Contains(t, err.Error(), "sortkeys[0] must not be nil")
	err = tb.MultiSetOpt(context.Background(), []byte("h1"), [][]byte{[]byte("")}, [][]byte{[]byte("v1")}, 0)
	assert.Nil(t, err)

	// MultiDel
	err = tb.MultiDel(context.Background(), nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	err = tb.MultiDel(context.Background(), []byte{}, nil)
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	err = tb.MultiDel(context.Background(), []byte("h1"), nil)
	assert.Contains(t, err.Error(), "sortkeys must not be nil")
	err = tb.MultiDel(context.Background(), []byte("h1"), [][]byte{})
	assert.Contains(t, err.Error(), "sortkeys must not be empty")
	err = tb.MultiDel(context.Background(), []byte("h1"), [][]byte{nil})
	assert.Contains(t, err.Error(), "sortkeys[0] must not be nil")

	// TTL
	_, err = tb.TTL(context.Background(), nil, nil)
	assert.Contains(t, err.Error(), "hashkey must not be nil")
	_, err = tb.TTL(context.Background(), []byte{}, nil)
	assert.Contains(t, err.Error(), "hashkey must not be empty")
	_, err = tb.TTL(context.Background(), []byte("h1"), nil)
	assert.Contains(t, err.Error(), "sortkey must not be nil")
	_, err = tb.TTL(context.Background(), []byte("h1"), []byte(""))
	assert.Nil(t, err)
}

func TestPegasusTableConnector_TriggerSelfUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	ptb := &pegasusTableConnector{
		tableName:    "temp",
		meta:         nil,
		replica:      nil,
		confUpdateCh: make(chan bool, 1),
		logger:       pegalog.GetLogger(),
	}

	confUpdate, retry, err := ptb.handleReplicaError(nil, nil) // no error
	assert.NoError(t, err)
	assert.False(t, confUpdate)
	assert.False(t, retry)

	confUpdate, retry, err = ptb.handleReplicaError(errors.New("not nil"), nil) // unknown error
	<-ptb.confUpdateCh                                                          // must trigger confUpdate
	assert.Error(t, err)
	assert.True(t, confUpdate)
	assert.True(t, retry)

	confUpdate, retry, err = ptb.handleReplicaError(base.ERR_OBJECT_NOT_FOUND, nil)
	<-ptb.confUpdateCh
	assert.Error(t, err)
	assert.True(t, confUpdate)
	assert.True(t, retry)

	confUpdate, retry, err = ptb.handleReplicaError(base.ERR_INVALID_STATE, nil)
	<-ptb.confUpdateCh
	assert.Error(t, err)
	assert.True(t, confUpdate)
	assert.False(t, retry)

	confUpdate, retry, err = ptb.handleReplicaError(base.ERR_PARENT_PARTITION_MISUSED, nil)
	<-ptb.confUpdateCh
	assert.Error(t, err)
	assert.True(t, confUpdate)
	assert.False(t, retry)

	{ // Ensure: The following errors should not trigger configuration update
		errorTypes := []error{base.ERR_TIMEOUT, context.DeadlineExceeded, base.ERR_CAPACITY_EXCEEDED, base.ERR_NOT_ENOUGH_MEMBER, base.ERR_BUSY, base.ERR_SPLITTING, base.ERR_DISK_INSUFFICIENT}

		for _, err := range errorTypes {
			channelEmpty := false
			confUpdate, retry, err = ptb.handleReplicaError(err, nil)
			select {
			case <-ptb.confUpdateCh:
			default:
				channelEmpty = true
			}
			assert.True(t, channelEmpty)

			assert.Error(t, err)
			assert.False(t, confUpdate)
			assert.False(t, retry)
		}
	}
}

func TestPegasusTableConnector_ValidateHashKey(t *testing.T) {
	hashKey := []byte(nil)
	assert.NotNil(t, validateHashKey(hashKey))

	hashKey = make([]byte, 0)
	assert.NotNil(t, validateHashKey(hashKey))

	hashKey = make([]byte, math.MaxUint16+1)
	assert.NotNil(t, validateHashKey(hashKey))
}

func TestPegasusTableConnector_HandleInvalidQueryConfigResp(t *testing.T) {
	defer leaktest.Check(t)()

	partitionCount := 8
	p := &pegasusTableConnector{
		tableName: "temp",
		parts:     make([]*replicaNode, partitionCount),
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
		assert.Equal(t, partitionCount, len(p.parts))
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_OK"}

		resp.Partitions = make([]*replication.PartitionConfiguration, 6)
		resp.PartitionCount = 6

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
		assert.Equal(t, partitionCount, len(p.parts))
	}

	{
		resp := replication.NewQueryCfgResponse()
		resp.Err = &base.ErrorCode{Errno: "ERR_OK"}

		resp.Partitions = make([]*replication.PartitionConfiguration, 2)
		resp.PartitionCount = 2

		err := p.handleQueryConfigResp(resp)
		assert.NotNil(t, err)
		assert.Equal(t, partitionCount, len(p.parts))
	}
}

func TestPegasusTableConnector_QueryConfigRespWhileStartSplit(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()
	ptb, _ := tb.(*pegasusTableConnector)

	partitionCount := len(ptb.parts)
	resp := replication.NewQueryCfgResponse()
	resp.Err = &base.ErrorCode{Errno: "ERR_OK"}
	resp.AppID = ptb.appID
	resp.PartitionCount = int32(partitionCount * 2)
	resp.Partitions = make([]*replication.PartitionConfiguration, partitionCount*2)
	for i := 0; i < partitionCount*2; i++ {
		if i < partitionCount {
			resp.Partitions[i] = ptb.parts[i].pconf
		} else {
			conf := replication.NewPartitionConfiguration()
			conf.Ballot = -1
			conf.Pid = &base.Gpid{ptb.appID, int32(i)}
			resp.Partitions[i] = conf
		}
	}

	err = ptb.handleQueryConfigResp(resp)
	assert.Nil(t, err)
	assert.Equal(t, partitionCount*2, len(ptb.parts))
	ptb.Close()
}

func TestPegasusTableConnector_QueryConfigRespWhileCancelSplit(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()
	ptb, _ := tb.(*pegasusTableConnector)

	partitionCount := len(ptb.parts)
	nodes := make([]*replicaNode, partitionCount*2)
	for i := 0; i < partitionCount*2; i++ {
		if i < partitionCount {
			nodes[i] = ptb.parts[i]
		} else {
			conf := replication.NewPartitionConfiguration()
			conf.Ballot = -1
			conf.Pid = &base.Gpid{ptb.appID, int32(i)}
			nodes[i] = &replicaNode{nil, conf}
		}
	}

	resp := replication.NewQueryCfgResponse()
	resp.Err = &base.ErrorCode{Errno: "ERR_OK"}
	resp.AppID = ptb.appID
	resp.PartitionCount = int32(partitionCount)
	resp.Partitions = make([]*replication.PartitionConfiguration, partitionCount)
	for i := 0; i < partitionCount; i++ {
		resp.Partitions[i] = ptb.parts[i].pconf
	}

	ptb.parts = make([]*replicaNode, partitionCount*2)
	ptb.parts = nodes

	err = ptb.handleQueryConfigResp(resp)
	assert.Nil(t, err)
	assert.Equal(t, partitionCount, len(ptb.parts))
	ptb.Close()
}

func TestPegasusTableConnector_Close(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	// Ensure: Closing table doesn't close the connections.

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	ptb, _ := tb.(*pegasusTableConnector)

	err = tb.Set(context.Background(), []byte("a"), []byte("a"), []byte("a"))
	assert.Nil(t, err)

	ptb.Close()
	_, r := ptb.getPartition(crc64Hash([]byte("a")))
	assert.Equal(t, r.ConnState(), rpc.ConnStateReady)
}

func TestPegasusTableConnector_GetPartitionIndex(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()
	ptb, _ := tb.(*pegasusTableConnector)

	// hashKey = 'a', partitionCount = 8, target index is 4
	targetIndex := 4
	partitionHash := crc64Hash([]byte("a"))
	gpid, _ := ptb.getPartition(partitionHash)
	assert.Equal(t, gpid.PartitionIndex, int32(targetIndex))
}

func TestPegasusTableConnector_GetPartitionIndexRedirct(t *testing.T) {
	// Ensure loopForAutoUpdate will be closed.
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()
	ptb, _ := tb.(*pegasusTableConnector)

	partitionCount := len(ptb.parts)
	nodes := make([]*replicaNode, partitionCount*2)
	for i := 0; i < partitionCount*2; i++ {
		if i < partitionCount {
			nodes[i] = ptb.parts[i]
		} else {
			conf := replication.NewPartitionConfiguration()
			conf.Ballot = -1
			conf.Pid = &base.Gpid{ptb.appID, int32(i)}
			nodes[i] = &replicaNode{nil, conf}
		}
	}
	ptb.parts = make([]*replicaNode, partitionCount*2)
	ptb.parts = nodes

	// hashKey = 'a', partitionCount = 16, target index is 12
	// But partition is invalid, it should redirect to parent partition, index is 4
	targetIndex := 4
	partitionHash := crc64Hash([]byte("a"))
	gpid, _ := ptb.getPartition(partitionHash)
	assert.Equal(t, gpid.PartitionIndex, int32(targetIndex))
}

func TestPegasusTableConnector_MultiKeyOperations(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	testMultiKeyOperations(t, tb)
}

func testMultiKeyOperations(t *testing.T, tb TableConnector) {
	hashKey := []byte("h1")

	sortKeys := make([][]byte, 10)
	values := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		// make sortKeys sorted.
		sidBuf := []byte(fmt.Sprintf("%d", i))
		var sidWithLeadingZero bytes.Buffer
		for k := 0; k < 20-len(sidBuf); k++ {
			sidWithLeadingZero.WriteByte('0')
		}
		sidWithLeadingZero.Write(sidBuf)
		sortKeys[i] = sidWithLeadingZero.Bytes()
		values[i] = []byte(fmt.Sprintf("v%d", i))
	}

	// clear keyspace
	results, allFetched, err := tb.MultiGetRange(context.Background(), hashKey, nil, nil)
	assert.NoError(t, err)
	assert.True(t, allFetched)
	for _, result := range results {
		assert.NoError(t, tb.Del(context.Background(), hashKey, result.SortKey))
	}
	count, err := tb.SortKeyCount(context.Background(), hashKey)
	assert.NoError(t, err)
	assert.Equal(t, count, int64(0))

	// empty database
	results, allFetched, err = tb.MultiGet(context.Background(), hashKey, sortKeys)
	assert.NoError(t, err)
	assert.Nil(t, results)
	assert.True(t, allFetched)
	results, allFetched, err = tb.MultiGetRange(context.Background(), hashKey, nil, nil)
	assert.NoError(t, err)
	assert.Nil(t, results)
	assert.True(t, allFetched)

	// === read after write === //

	assert.NoError(t, tb.MultiSet(context.Background(), hashKey, sortKeys, values))

	results, allFetched, err = tb.MultiGet(context.Background(), hashKey, sortKeys)
	assert.NoError(t, err)
	assert.Equal(t, len(results), len(values))
	for i, result := range results {
		assert.Equal(t, result.Value, values[i])
		assert.Equal(t, result.SortKey, sortKeys[i])
	}
	assert.True(t, allFetched)

	count, err = tb.SortKeyCount(context.Background(), hashKey)
	assert.NoError(t, err)
	assert.Equal(t, count, int64(len(sortKeys)))

	// test StartInclusive & StopInclusive

	results, allFetched, err = tb.MultiGetRangeOpt(context.Background(), hashKey, sortKeys[0], sortKeys[len(sortKeys)-1],
		&MultiGetOptions{StartInclusive: true, StopInclusive: true})
	assert.NoError(t, err)
	assert.Equal(t, len(results), len(values))
	for i, result := range results {
		assert.Equal(t, result.Value, values[i])
		assert.Equal(t, result.SortKey, sortKeys[i])
	}
	assert.True(t, allFetched)

	results, allFetched, err = tb.MultiGetRangeOpt(context.Background(), hashKey, sortKeys[0], sortKeys[len(sortKeys)-1],
		&MultiGetOptions{StartInclusive: false, StopInclusive: false})
	assert.NoError(t, err)
	assert.Equal(t, len(results), len(values)-2) // exclude start and stop
	for i, result := range results {
		assert.Equal(t, result.Value, values[i+1])
		assert.Equal(t, result.SortKey, sortKeys[i+1])
	}
	assert.True(t, allFetched)

	// test MaxFetchCount
	results, allFetched, err = tb.MultiGetOpt(context.Background(), hashKey, sortKeys, &MultiGetOptions{MaxFetchCount: 4})
	assert.NoError(t, err)
	assert.Equal(t, len(results), 4)
	assert.False(t, allFetched)

	// test MaxFetchSize
	results, allFetched, err = tb.MultiGetOpt(context.Background(), hashKey, sortKeys, &MultiGetOptions{MaxFetchSize: len(values[0])})
	assert.NoError(t, err)
	assert.Equal(t, len(results), 1)
	assert.False(t, allFetched)

	// ensure passing nil to `sortKeys` in MultiGet retrieves all entries
	results, allFetched, err = tb.MultiGetOpt(context.Background(), hashKey, nil, &MultiGetOptions{})
	assert.NoError(t, err)
	assert.Equal(t, len(results), len(sortKeys))
	assert.True(t, allFetched)

	// test Reverse
	results, allFetched, err = tb.MultiGetRangeOpt(context.Background(), hashKey, nil, nil, &MultiGetOptions{Reverse: true, MaxFetchCount: 1})
	assert.NoError(t, err)
	assert.Equal(t, allFetched, false)
	assert.Equal(t, results, []*KeyValue{
		{SortKey: sortKeys[len(sortKeys)-1], Value: values[len(sortKeys)-1]},
	})

	// test NoValue
	results, allFetched, err = tb.MultiGetRangeOpt(context.Background(), hashKey, nil, nil, &MultiGetOptions{NoValue: true, MaxFetchCount: 1})
	assert.NoError(t, err)
	assert.False(t, allFetched)
	assert.Equal(t, results, []*KeyValue{
		{SortKey: sortKeys[0], Value: []byte("")},
	})

	// === ttl === //

	assert.NoError(t, tb.MultiSetOpt(context.Background(), hashKey, sortKeys, values, 10*time.Second))
	for _, sortKey := range sortKeys {
		ttl, err := tb.TTL(context.Background(), hashKey, sortKey)
		assert.NoError(t, err)
		assert.Condition(t, func() bool {
			// pegasus server may return a ttl slightly different
			// from the value we set.
			return ttl <= 11 && ttl >= 9
		})
	}

	// test with invalid ttl
	assert.Error(t, tb.MultiSetOpt(context.Background(), hashKey, sortKeys, values, -1*time.Second))
}

func TestPegasusTableConnector_CheckAndSet(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	{ // CheckTypeValueNotExist
		// if (h1, s1) not exists, insert (s1, v1)
		err := tb.Del(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		res, err := tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueNotExist, []byte(""), []byte("s1"), []byte("v1"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, false)

		// since (h1, s1) exists, insertion of (s1, v1) failed
		res, err = tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueNotExist, []byte(""), []byte("s1"), []byte("v1"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, false)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, true)
		assert.Equal(t, res.CheckValue, []byte("v1"))
	}

	{ // CheckTypeValueExist
		// if (h1, s1) exists, insert (s1, v1)
		// this op will failed since there's no such entry.
		assert.Nil(t, tb.Del(context.Background(), []byte("h1"), []byte("s1")))
		res, err := tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s1"), []byte("v1"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, false)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, false)

		assert.Nil(t, tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1")))
		res, err = tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s1"), []byte("v2"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, true)
		assert.Equal(t, res.CheckValue, []byte("v1"))

		value, err := tb.Get(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Equal(t, value, []byte("v2"))

		// set ttl to 10 if value exists
		ttl, err := tb.TTL(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Equal(t, ttl, -1) // ttl is not set

		res, err = tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s1"), []byte("v3"),
			&CheckAndSetOptions{SetValueTTLSeconds: 10})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, false)
		assert.Equal(t, res.CheckValueExist, false) // no check value returned

		ttl, err = tb.TTL(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Condition(t, func() bool {
			return ttl >= 9 && ttl <= 11
		})
	}

	{ // check sortkey and set sortkey are different
		results, _, err := tb.MultiGetRange(context.Background(), []byte("h1"), nil, nil)
		assert.Nil(t, err)
		for _, result := range results {
			assert.Nil(t, tb.Del(context.Background(), []byte("h1"), result.SortKey))
		}

		assert.Nil(t, tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1")))
		res, err := tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s2"), []byte("v2"),
			&CheckAndSetOptions{ReturnCheckValue: true})
		assert.Nil(t, err)
		assert.Equal(t, res.SetSucceed, true)
		assert.Equal(t, res.CheckValueReturned, true)
		assert.Equal(t, res.CheckValueExist, true)
		assert.Equal(t, res.CheckValue, []byte("v1"))

		count, err := tb.SortKeyCount(context.Background(), []byte("h1"))
		assert.Nil(t, err)
		assert.Equal(t, count, int64(2))

		value, err := tb.Get(context.Background(), []byte("h1"), []byte("s1"))
		assert.Nil(t, err)
		assert.Equal(t, value, []byte("v1"))

		value, err = tb.Get(context.Background(), []byte("h1"), []byte("s2"))
		assert.Nil(t, err)
		assert.Equal(t, value, []byte("v2"))
	}

	// test with invalid ttl
	{
		_, err := tb.CheckAndSet(context.Background(), []byte("h1"), []byte("s1"), CheckTypeValueExist, []byte(""), []byte("s2"), []byte("v2"),
			&CheckAndSetOptions{SetValueTTLSeconds: -1})
		assert.Error(t, err)
	}

	// TODO(wutao1): add tests for other check type
}

func TestPegasusTableConnector_Incr(t *testing.T) {
	defer leaktest.Check(t)()

	concurrency := 3
	times := 20

	{
		client := NewClient(testingCfg)
		tb, err := client.OpenTable(context.Background(), "temp")
		assert.NoError(t, err)
		err = tb.Del(context.Background(), []byte("idx_hash"), []byte("idx_sort"))
		assert.NoError(t, err)
		_ = tb.Close()
		_ = client.Close()
	}

	sortedIDs := make([]int64, 0, times*concurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			client := NewClient(testingCfg)
			tb, err := client.OpenTable(context.Background(), "temp")
			assert.NoError(t, err)

			ids := make([]int64, 0, times)
			for i := 0; i < times; i++ {
				value, err := tb.Incr(context.Background(), []byte("idx_hash"), []byte("idx_sort"), 1)
				assert.NoError(t, err)
				ids = append(ids, value)
			}
			mu.Lock()
			sortedIDs = append(sortedIDs, ids...)
			mu.Unlock()
			_ = tb.Close()
			_ = client.Close()
		}()
	}
	wg.Wait()

	sort.Slice(sortedIDs, func(i, j int) bool {
		return sortedIDs[i] < sortedIDs[j]
	})

	for i := 0; i < times*concurrency; i++ {
		assert.Equal(t, int64(i+1), sortedIDs[i])
	}
}

func TestPegasusTableConnector_BatchGet(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	err = tb.Del(context.Background(), []byte("h1"), []byte("s1"))
	assert.Nil(t, err)
	err = tb.Del(context.Background(), []byte("h2"), []byte("s2"))
	assert.Nil(t, err)
	err = tb.Del(context.Background(), []byte("h3"), []byte("s3"))
	assert.Nil(t, err)

	err = tb.Set(context.Background(), []byte("h1"), []byte("s1"), []byte("v1"))
	assert.Nil(t, err)
	err = tb.Set(context.Background(), []byte("h2"), []byte("s2"), []byte("v2"))
	assert.Nil(t, err)
	err = tb.Set(context.Background(), []byte("h3"), []byte("s3"), []byte("v3"))
	assert.Nil(t, err)

	keys := []CompositeKey{{HashKey: []byte("h1"), SortKey: []byte("s1")},
		{HashKey: []byte("h2"), SortKey: []byte("s2")},
		{HashKey: []byte("h3"), SortKey: []byte("s3")}}
	values, err := tb.BatchGet(context.Background(), keys)
	assert.Nil(t, err)
	assert.Equal(t, values, [][]byte{[]byte("v1"), []byte("v2"), []byte("v3")})

	values, err = tb.BatchGet(context.Background(), nil)
	assert.Nil(t, values)
	assert.Equal(t, err.Error(),
		"pegasus BATCH_GET failed: InvalidParameter: CompositeKeys must not be nil")

	values, err = tb.BatchGet(context.Background(), []CompositeKey{{HashKey: []byte{}, SortKey: nil}, {HashKey: nil, SortKey: nil}})
	assert.Equal(t, values, [][]byte{nil, nil})
	if err.Error() != "pegasus BATCH_GET failed: [pegasus GET failed: InvalidParameter: hashkey must not be empty, pegasus GET failed: InvalidParameter: hashkey must not be nil]" &&
		err.Error() != "pegasus BATCH_GET failed: [pegasus GET failed: InvalidParameter: hashkey must not be nil, pegasus GET failed: InvalidParameter: hashkey must not be empty]" {
		assert.NotNil(t, nil) // ordering of the errors is indefinite
	}
}
