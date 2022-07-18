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
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/apache/incubator-pegasus/go-client/idl/rrdb"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/apache/incubator-pegasus/go-client/rpc"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func newFakeNodeSession(reader io.Reader, writer io.Writer) *nodeSession {
	n := newNodeSessionAddr("", NodeTypeMeta)
	n.conn = rpc.NewFakeRpcConn(reader, writer)
	n.codec = &MockCodec{}
	return n
}

func newMetaSession(addr string) *metaSession {
	return &metaSession{
		NodeSession: newNodeSession(addr, NodeTypeMeta),
		logger:      pegalog.GetLogger(),
	}
}

// This test verifies that no routine leaks after calling Close.
func TestNodeSession_Close(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, writer)

	n.tom.Go(n.loopForDialing)
	n.tom.Go(n.loopForRequest)
	n.tom.Go(n.loopForResponse)
	n.Close()
}

// This test ensures loopForRequest receives the PegasusRpcCall sent from
// CallWithGpid.
func TestNodeSession_LoopForRequest(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, writer)

	n.tom.Go(n.loopForRequest)

	go func() {
		n.CallWithGpid(context.Background(), nil, 0, nil, "")
	}()

	time.Sleep(time.Second)
	n.Close() // retrieving pendingResp is thread-unsafe, we must close the session first
	assert.Equal(t, 1, len(n.pendingResp))
}

// Ensure that giving an available address,
// dial will end up with success within 200ms
// and without goroutine leaks.
func TestNodeSession_LoopForDialingSuccess(t *testing.T) {
	defer leaktest.Check(t)()

	addr := "www.baidu.com:80"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(addr)

	n.tom.Go(n.loopForDialing)

	// ensure dial never kills the tomb
	n.tom.Go(func() error {
		<-n.tom.Dying()
		return nil
	})

	time.Sleep(time.Millisecond * 200)
	assert.True(t, n.tom.Alive())

	n.Close()
}

// Ensure that dial can be cancelled properly
// when nodeSession is closed.
func TestNodeSession_LoopForDialingCancelled(t *testing.T) {
	defer leaktest.Check(t)()

	addr := "www.baidu.com:12321"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(addr)

	n.tom.Go(n.loopForDialing)
	n.tryDial()

	time.Sleep(time.Second)
	// time.Second < rpc.ConnDialTimeout, it must still be connecting.
	assert.Equal(t, rpc.ConnStateConnecting, n.conn.GetState())

	time.Sleep(rpc.ConnDialTimeout) // dial failed.
	assert.Equal(t, rpc.ConnStateTransientFailure, n.conn.GetState())
	n.Close()
}

type IOErrWriter struct {
	err error
}

func (writer *IOErrWriter) Write(p []byte) (n int, err error) {
	return 0, writer.err
}

// Ensure if write failed eventually, the session will be stopped,
// and the rpc caller will be notified immediately with the write
// error.
func TestNodeSession_WriteFailed(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, &IOErrWriter{err: base.ERR_INVALID_STATE})
	defer n.Close()

	n.tom.Go(n.loopForRequest)

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		return []byte("a"), nil
	})
	n.codec = mockCodec

	_, err := n.CallWithGpid(context.Background(), &base.Gpid{}, 0, arg, "RPC_NAME")
	assert.NotNil(t, err)
	assert.Equal(t, n.conn.GetState(), rpc.ConnStateTransientFailure)
}

func TestNodeSession_WaitUntilSessionReady(t *testing.T) {
	defer leaktest.Check(t)()

	func() {
		n := newNodeSession("www.baidu.com:12321", "meta")
		defer n.Close()

		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
		defer cancel()
		err := n.waitUntilSessionReady(ctx)

		// timeout waiting for dialing
		assert.NotNil(t, err)
	}()

	func() {
		n := newNodeSession("0.0.0.0:8800", "meta")
		defer n.Close()

		err := n.waitUntilSessionReady(context.Background())
		assert.Nil(t, err)
	}()
}

// In this test we send the rpc request to an echo server,
// and verify that if nodeSession correctly receives the response.
func TestNodeSession_CallToEcho(t *testing.T) {
	defer leaktest.Check(t)()

	// start echo server first
	n := newNodeSession("0.0.0.0:8800", NodeTypeMeta)
	defer n.Close()

	var expected []byte
	var actual []byte

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		expected, _ = new(PegasusCodec).Marshal(v)
		buf := make([]byte, len(expected)+4)

		// prefixed with length
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))
		copy(buf[4:], expected)

		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		actual = data
		r, _ := v.(*PegasusRpcCall)
		r.SeqId = 1
		r.Result = nameToResultMap["RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX_ACK"]()
		return nil
	})

	n.codec = mockCodec

	// wait for connection becoming ready
	time.Sleep(time.Millisecond * 500)

	meta := &metaSession{NodeSession: n, logger: pegalog.GetLogger()}
	meta.queryConfig(context.Background(), "temp")
	assert.Equal(t, expected, actual)
}

// Ensure that concurrent calls won't cause error.
// The only difference between this test and TestNodeSession_ConcurrentCall is
// that it sends rpc to echo server rather than meta server.
func TestNodeSession_ConcurrentCallToEcho(t *testing.T) {
	defer leaktest.Check(t)()

	// start echo server first
	n := newNodeSession("0.0.0.0:8800", NodeTypeMeta)

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		r, _ := v.(*PegasusRpcCall)
		marshaled, _ := new(PegasusCodec).Marshal(r)

		// prefixed with length
		buf := make([]byte, 4+len(marshaled))
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))
		copy(buf[4:], marshaled)

		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		r, _ := v.(*PegasusRpcCall)

		assert.True(t, len(data) > thriftHeaderBytesLen)
		data = data[thriftHeaderBytesLen:]
		iprot := thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(bytes.NewBuffer(data)))
		_, _, seqId, err := iprot.ReadMessageBegin()
		if err != nil {
			return err
		}
		r.SeqId = seqId
		r.Result = rrdb.NewMetaQueryCfgResult()

		return nil
	})
	n.codec = mockCodec

	// wait for connection becoming ready
	time.Sleep(time.Millisecond * 500)

	meta := &metaSession{NodeSession: n, logger: pegalog.GetLogger()}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			// to increase concurrency
			time.Sleep(100 * time.Millisecond)

			meta.queryConfig(context.Background(), "temp")
			wg.Done()
		}()
	}
	wg.Wait()

	n.Close()
}

// please ensure there's no active port on 8801.
func TestNodeSession_GracefulShutdown(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:8801")

	go func() {
		// the call should block infinitely if no one close the session.
		meta.queryConfig(context.Background(), "temp")
	}()

	time.Sleep(time.Millisecond * 500)
	meta.Close()
}

func TestNodeSession_RestartConnection(t *testing.T) {
	defer leaktest.Check(t)()

	meta := newMetaSession("0.0.0.0:34601")
	_, err := meta.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	meta.Close()

	meta = newMetaSession("0.0.0.0:34601")
	_, err = meta.queryConfig(context.Background(), "temp")
	assert.Nil(t, err)
	meta.Close()
}

func TestNodeSession_ReceiveErrorCode(t *testing.T) {
	defer leaktest.Check(t)()

	n := newNodeSession("0.0.0.0:8800", NodeTypeMeta)
	defer n.Close()

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()

	mockCodec := &MockCodec{}
	n.codec = mockCodec

	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		// prefixed with length
		buf := make([]byte, 4+1)
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))

		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		r, _ := v.(*PegasusRpcCall)
		r.SeqId = 1
		r.Err = base.ERR_INVALID_STATE
		return nil
	})

	result, err := n.CallWithGpid(context.Background(), &base.Gpid{}, 0, arg, "RPC_NAME")
	assert.Equal(t, result, nil)
	assert.Equal(t, err, base.ERR_INVALID_STATE)
}

// Ensure nodeSession will redial when user calls an rpc through it.
func TestNodeSession_Redial(t *testing.T) {
	defer leaktest.Check(t)()

	addr := "0.0.0.0:8800"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(addr)
	defer n.Close()

	n.tom.Go(n.loopForDialing)

	// simulate the condition where loopForRequest or loopForResponse died
	// due to io failure.
	n.tom.Go(func() error {
		return nil
	})
	time.Sleep(time.Second)

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		// prefixed with length
		buf := make([]byte, 4+1)
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))
		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		r, _ := v.(*PegasusRpcCall)
		r.SeqId = 1
		r.Err = base.ERR_INVALID_STATE
		return nil
	})
	n.codec = mockCodec

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()
	_, err := n.CallWithGpid(context.Background(), &base.Gpid{}, 0, arg, "RPC_NAME")

	assert.Equal(t, n.ConnState(), rpc.ConnStateReady)
	assert.Equal(t, err, base.ERR_INVALID_STATE)
}

func TestNodeSession_ReadEOF(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, writer)
	n.tom.Go(n.loopForResponse)

	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, n.ConnState(), rpc.ConnStateTransientFailure)
}
