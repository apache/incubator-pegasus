// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"git.apache.org/thrift.git/lib/go/thrift"
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/rpc"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func newFakeNodeSession(reader io.Reader, writer io.Writer) *nodeSession {
	n := newNodeSessionAddr("", kNodeTypeMeta)
	n.conn = rpc.NewFakeRpcConn(n.tom, reader, writer)
	n.codec = &MockCodec{}
	return n
}

// This test verifies that no routine leaks
// if n.Close is called before looping.
func TestNodeSession_Close(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, writer)
	n.Close()

	n.tom.Go(n.loopForRequest)
	n.tom.Go(n.loopForResponse)
	n.tom.Go(n.loopForDialing)
}

// This test ensures loopForRequest receives the rpcCall sent from
// callWithGpid.
func TestNodeSession_LoopForRequest(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, writer)
	defer n.Close()

	n.tom.Go(n.loopForRequest)

	go func() {
		n.callWithGpid(context.Background(), nil, nil, "")
	}()

	time.Sleep(time.Second)
	assert.Equal(t, 1, len(n.pendingResp))
}

// Ensure that giving an available address,
// dial will end up with success within 200ms
// and without goroutine leaks.
func TestNodeSession_LoopForDialingSuccess(t *testing.T) {
	defer leaktest.Check(t)()

	addr := "www.baidu.com:80"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(n.tom, addr)

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
	n.conn = rpc.NewRpcConn(n.tom, addr)

	n.tom.Go(n.loopForDialing)

	time.Sleep(time.Second)
	// time.Second < rpc.RpcConnDialTimeout, it must still be connecting.
	assert.Equal(t, rpc.ConnStateConnecting, n.conn.GetState())
	n.conn.Close()
}

func TestNodeSession_LoopForDialingRetry(t *testing.T) {
	defer leaktest.CheckTimeout(t, rpc.RpcConnDialTimeout*2)()

	addr := "www.baidu.com:12321"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(n.tom, addr)

	n.tom.Go(n.loopForDialing)

	time.Sleep(rpc.RpcConnDialTimeout + 100*time.Millisecond)
	assert.Equal(t, rpc.ConnStateTransientFailure, n.conn.GetState())

	// Ensure that dial will reconnect if the last try failed.
	time.Sleep(time.Second * 2)
	assert.Equal(t, rpc.ConnStateConnecting, n.conn.GetState())

	n.conn.Close()
}

type IOErrWriter struct {
	err error
}

func (writer *IOErrWriter) Write(p []byte) (n int, err error) {
	return 0, writer.err
}

type IOErrReader struct {
	err error
}

func (reader *IOErrReader) Read(p []byte) (n int, err error) {
	return 0, reader.err
}

// Ensure if write failed eventually, the session will be stopped,
// and the rpc caller will be notified immediately with the write
// error.
func TestNodeSession_WriteFailed(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, &IOErrWriter{err: base.ERR_CLIENT_FAILED})
	n.tom.Go(n.loopForRequest)

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()

	_, err := n.callWithGpid(context.Background(), &base.Gpid{0, 0}, arg, "RPC_NAME")
	assert.Equal(t, err, base.ERR_CLIENT_FAILED)
	assert.Equal(t, n.conn.GetState(), rpc.ConnStateTransientFailure)
}

// Ensure if read failed due to un-retryable error,
// the session will be shutdown.
func TestNodeSession_ReadFailed(t *testing.T) {
	defer leaktest.Check(t)()

	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(&IOErrReader{err: base.ERR_CLIENT_FAILED}, writer)
	n.tom.Go(n.loopForRequest)
	n.tom.Go(n.loopForResponse)

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()

	_, err := n.callWithGpid(context.Background(), &base.Gpid{0, 0}, arg, "RPC_NAME")
	assert.Equal(t, err, context.Canceled)
	assert.Equal(t, n.conn.GetState(), rpc.ConnStateTransientFailure)
}

func TestCodec_Marshal(t *testing.T) {
	expected := []byte{
		0x54, 0x48, 0x46, 0x54, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x30, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x4a, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
		0x80, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x26,
		0x52, 0x50, 0x43, 0x5f, 0x43, 0x4d, 0x5f, 0x51,
		0x55, 0x45, 0x52, 0x59, 0x5f, 0x50, 0x41, 0x52,
		0x54, 0x49, 0x54, 0x49, 0x4f, 0x4e, 0x5f, 0x43,
		0x4f, 0x4e, 0x46, 0x49, 0x47, 0x5f, 0x42, 0x59,
		0x5f, 0x49, 0x4e, 0x44, 0x45, 0x58, 0x00, 0x00,
		0x00, 0x01, 0x0c, 0x00, 0x01, 0x0b, 0x00, 0x01,
		0x00, 0x00, 0x00, 0x04, 0x74, 0x65, 0x6d, 0x70,
		0x0f, 0x00, 0x02, 0x08, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x00,
	}
	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()
	arg.Query.AppName = "temp"
	arg.Query.PartitionIndices = []int32{}

	r := &rpcCall{
		args:  arg,
		name:  "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX",
		gpid:  &base.Gpid{0, 0},
		seqId: 1,
	}

	actual, _ := PegasusCodec{}.Marshal(r)
	assert.Equal(t, expected, actual)
}

// In this test we send the rpc request to an echo server,
// and verify that if nodeSession correctly receives the response.
func TestNodeSession_CallToEcho(t *testing.T) {
	defer leaktest.Check(t)()

	// start echo server first
	n := newMetaSession("0.0.0.0:7")
	defer n.Close()

	var expected []byte
	var actual []byte

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		expected, _ = PegasusCodec{}.Marshal(v)
		buf := make([]byte, len(expected)+4)

		// prefixed with length
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))
		copy(buf[4:], expected)

		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		actual = data
		r, _ := v.(*rpcCall)
		r.seqId = 1
		r.result = nameToResultMap["RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX_ACK"]()
		return nil
	})

	n.codec = mockCodec

	// wait for connection becoming ready
	time.Sleep(time.Millisecond * 500)

	n.queryConfig(context.Background(), "temp")
	assert.Equal(t, expected, actual)
}

// Ensure that concurrent calls won't cause error.
// The only difference between this test and TestNodeSession_ConcurrentCall is
// that it sends rpc to echo server rather than meta server.
func TestNodeSession_ConcurrentCallToEcho(t *testing.T) {
	defer leaktest.Check(t)()

	// start echo server first
	meta := newMetaSession("0.0.0.0:7")

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		r, _ := v.(*rpcCall)
		marshaled, _ := PegasusCodec{}.Marshal(r)

		// prefixed with length
		buf := make([]byte, 4+len(marshaled))
		binary.BigEndian.PutUint32(buf, uint32(len(buf)))
		copy(buf[4:], marshaled)

		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		r, _ := v.(*rpcCall)

		assert.True(t, len(data) > thriftHeaderBytesLen)
		data = data[thriftHeaderBytesLen:]
		iprot := thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(bytes.NewBuffer(data)))
		_, _, seqId, err := iprot.ReadMessageBegin()
		if err != nil {
			return err
		}
		r.seqId = seqId
		r.result = rrdb.NewMetaQueryCfgResult()

		return nil
	})
	meta.codec = mockCodec

	// wait for connection becoming ready
	time.Sleep(time.Millisecond * 500)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			meta.queryConfig(context.Background(), "temp")
			wg.Done()
		}()
	}
	wg.Wait()

	meta.Close()
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

func TestNodeSession_RedialTooOften(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second*3+rpc.RpcConnDialTimeout*2)

	addr := "www.baidu.com:12321"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(n.tom, addr)

	t1 := time.Now()
	n.dial() // the first call must fail

	n.dial() // wait 2 secs
	t2 := time.Now()

	assert.True(t, t2.Sub(t1) > time.Second*2+rpc.RpcConnDialTimeout*2)
	assert.True(t, t2.Sub(t1) < time.Second*3+rpc.RpcConnDialTimeout*2)

	n.Close()
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
