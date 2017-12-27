// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/pegasus-kv/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/pegasus-go-client/idl/replication"
	"github.com/pegasus-kv/pegasus-go-client/idl/rrdb"
	"github.com/pegasus-kv/pegasus-go-client/rpc"
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

// This test ensures that loopForResponse should remove
// request from pending queue when it receives the corresponding
// response.
func TestNodeSession_LoopForResponse(t *testing.T) {
	defer leaktest.Check(t)()

	reader := bytes.NewBuffer(make([]byte, 0))
	writer := bytes.NewBuffer(make([]byte, 0))
	n := newFakeNodeSession(reader, writer)
	defer n.Close()

	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, 0)
	reader.Write(buf)

	// reading a response with seqid = 1
	codec := &MockCodec{}
	codec.MockUnMarshal(func(data []byte, v interface{}) error {
		r, _ := v.(*rpcCall)
		r.seqId = 1
		return nil
	})
	n.codec = codec

	// pending for that response
	recv := &reqItem{
		ch:   make(chan *reqItem),
		call: &rpcCall{},
		err:  nil,
	}
	n.pendingResp[1] = recv

	n.tom.Go(n.loopForResponse)
	recv = <-recv.ch
	assert.Equal(t, 0, len(n.pendingResp))
}

// Ensure that giving an available address,
// loopForDialing will end up with success within 200ms
// and without goroutine leaks.
func TestNodeSession_LoopForDialingSuccess(t *testing.T) {
	defer leaktest.Check(t)()

	addr := "www.baidu.com:80"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(n.tom, addr)

	n.tom.Go(n.loopForDialing)

	// ensure loopForDialing never kills the tomb
	n.tom.Go(func() error {
		<-n.tom.Dying()
		return nil
	})

	time.Sleep(time.Millisecond * 200)
	assert.True(t, n.tom.Alive())

	n.Close()
}

// Ensure that loopForDialing can be cancelled properly
// when nodeSession is closed.
func TestNodeSession_LoopForDialingCancelled(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second*5)()

	addr := "www.baidu.com:12321"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(n.tom, addr)

	n.tom.Go(n.loopForDialing)

	time.Sleep(time.Second)
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

	// Ensure that loopForDialing will reconnect if the last try failed.
	time.Sleep(time.Second)
	assert.Equal(t, rpc.ConnStateConnecting, n.conn.GetState())

	n.conn.Close()
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
	addr := "0.0.0.0:8800"
	n := newNodeSessionAddr(addr, "meta")
	n.conn = rpc.NewRpcConn(n.tom, addr)

	var expected []byte
	var actual []byte

	mockCodec := &MockCodec{}
	mockCodec.MockMarshal(func(v interface{}) ([]byte, error) {
		expected, _ = PegasusCodec{}.Marshal(v)
		buf := make([]byte, len(expected)+4)

		// prefixed with length
		binary.BigEndian.PutUint32(buf, uint32(len(expected)))
		copy(buf[4:], expected)

		return buf, nil
	})
	mockCodec.MockUnMarshal(func(data []byte, v interface{}) error {
		actual = data
		r, _ := v.(*rpcCall)
		r.seqId = 1
		return nil
	})

	n.codec = mockCodec
	assert.Nil(t, n.conn.TryConnect())

	n.tom.Go(n.loopForRequest)
	n.tom.Go(n.loopForResponse)

	arg := rrdb.NewMetaQueryCfgArgs()
	arg.Query = replication.NewQueryCfgRequest()
	n.callWithGpid(context.Background(), &base.Gpid{0, 0}, arg, "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX")

	assert.Equal(t, expected, actual)
	n.Close()
}
