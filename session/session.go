// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"fmt"
	"github.com/pegasus-kv/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/pegasus-go-client/pegalog"
	"github.com/pegasus-kv/pegasus-go-client/rpc"
	"gopkg.in/tomb.v2"
)

// nodeSession represents the network session to a node
// (either a meta server or a replica server).
// It encapsulates the internal rpc process, including
// network communication and message (de)serialization.
type nodeSession struct {
	logger pegalog.Logger

	// atomic incremented counter that ensures each rpc
	// has a unique sequence id
	seqId int32

	addr  string
	ntype nodeType
	conn  *rpc.RpcConn

	tom *tomb.Tomb

	reqc        chan *reqItem
	pendingResp map[int32]*reqItem
	mu          sync.Mutex

	codec rpc.Codec
}

type reqItem struct {
	ch   chan *reqItem
	call *rpcCall
	err  error
}

type nodeType string

const (
	kNodeTypeMeta    nodeType = "meta"
	kNodeTypeReplica          = "replica"
)

func newNodeSessionAddr(addr string, ntype nodeType) *nodeSession {
	return &nodeSession{
		logger:      pegalog.GetLogger(),
		ntype:       ntype,
		seqId:       0,
		codec:       PegasusCodec{},
		pendingResp: make(map[int32]*reqItem),
		reqc:        make(chan *reqItem),
		addr:        addr,
		tom:         &tomb.Tomb{},
	}
}

// newNodeSession always returns a non-nil value even when the
// connection attempt failed.
// Each nodeSession corresponds to an RpcConn.
func newNodeSession(addr string, ntype nodeType) *nodeSession {
	logger := pegalog.GetLogger()
	logger.Printf("create session with %s(%s)", addr, ntype)

	n := newNodeSessionAddr(addr, ntype)
	n.conn = rpc.NewRpcConn(n.tom, addr)

	n.tom.Go(n.loopForDialing)
	return n
}

func (n *nodeSession) Ready() bool {
	return n.conn.GetState() == rpc.ConnStateReady
}

func (n *nodeSession) String() string {
	return fmt.Sprintf("addr: %s, nodetype: %s, pending rpc: %d", n.addr, n.ntype, len(n.pendingResp))
}

// loop in background if the connection is not yet established.
func (n *nodeSession) loopForDialing() error {
	for {
		select {
		case <-n.tom.Dying():
		default:
			err := n.conn.TryConnect()
			if err != nil {
				n.logger.Printf("failed to dial %s: %s", n.ntype, err)

				select {
				case <-n.tom.Dying():
				default:
					// retry 1 sec later.
					time.Sleep(time.Second)
					continue
				}
			} else {
				n.tom.Go(n.loopForRequest)
				n.tom.Go(n.loopForResponse)
			}
		}

		n.logger.Printf("stop dialing for %s: %s, connection state: %s", n.ntype, n.addr, n.conn.GetState())
		return nil
	}
}

// single-routine worker used for sending requests.
func (n *nodeSession) loopForRequest() error {
	for {
		select {
		case <-n.tom.Dying():
			return n.tom.Err()
		case item := <-n.reqc:
			n.mu.Lock()
			n.pendingResp[item.call.seqId] = item
			n.mu.Unlock()

			n.writeRequest(item.call)
		}
	}
}

// single-routine worker used for reading response.
// We register a map of sequence id -> recvItem when each request comes,
// so that when a response is received, we are able to notify its caller.
func (n *nodeSession) loopForResponse() error {
	for {
		select {
		case <-n.tom.Dying():
			return n.tom.Err()
		default:
		}

		call, err := n.readResponse()

		if err != nil {
			if rpc.IsRetryableError(err) {
				// TODO(wutao1) abstract the retry strategy
				select {
				case <-time.After(time.Second):
					continue
				case <-n.tom.Dying():
					return n.tom.Err()
				}
			} else {
				n.logger.Printf("failed to read response from [%s]: %s", n.ntype, err)
				return err
			}
		}

		n.mu.Lock()
		item, ok := n.pendingResp[call.seqId]
		n.mu.Unlock()

		if !ok {
			n.logger.Printf("ignore stale response (seqId: %d) from [%s, %s]: %s",
				call.seqId, n.ntype, n.addr, call.result)
			continue
		}

		item.call.result = call.result

		select {
		// notify the caller
		case item.ch <- item:
			n.mu.Lock()
			delete(n.pendingResp, call.seqId)
			n.mu.Unlock()
		case <-n.tom.Dying():
			return n.tom.Err()
		}
	}
}

// Invoke a rpc call.
func (n *nodeSession) callWithGpid(ctx context.Context, gpid *base.Gpid, args rpcRequestArgs, name string) (result rpcResponseResult, err error) {
	rcall := &rpcCall{}
	rcall.args = args
	rcall.name = name
	rcall.seqId = atomic.AddInt32(&n.seqId, 1) // increment sequence id
	rcall.gpid = gpid

	item := &reqItem{call: rcall, ch: make(chan *reqItem)}

	// either the ctx cancelled or the tomb killed will stop this rpc call.
	ctxWithTomb := n.tom.Context(ctx)
	select {
	// passes the request to loopForRequest
	case n.reqc <- item:
		select {
		// receive from loopForResponse
		case item = <-item.ch:
			err = item.err
			result = rcall.result
			return
		case <-ctxWithTomb.Done():
			err = ctxWithTomb.Err()
			result = nil
			return
		}
	case <-ctxWithTomb.Done():
		err = ctxWithTomb.Err()
		result = nil
		return
	}
}

func (n *nodeSession) writeRequest(r *rpcCall) error {
	bytes, err := n.codec.Marshal(r)
	if err != nil {
		return err
	}
	return n.conn.Write(bytes)
}

// readResponse returns nil rpcCall if error encountered.
func (n *nodeSession) readResponse() (*rpcCall, error) {
	// read length field
	lenBuf, err := n.conn.Read(4)
	if err != nil && len(lenBuf) < 4 {
		return nil, err
	}
	resplen := binary.BigEndian.Uint32(lenBuf)
	if resplen < 4 {
		return nil, fmt.Errorf("response length(%d) smaller than 4 bytes", resplen)
	}
	resplen -= 4 // 4 bytes for length

	// read data field
	buf, err := n.conn.Read(int(resplen))
	if err != nil && len(buf) != int(resplen) {
		return nil, err
	}

	r := &rpcCall{}
	if err := n.codec.Unmarshal(buf, r); err != nil {
		n.logger.Printf("failed to unmarshal response [%s, %s]: %s", n.ntype, n.addr, err)
		return nil, err
	}
	return r, nil
}

func (n *nodeSession) Close() error {
	n.conn.Close()
	n.tom.Kill(errors.New("nodeSession closed"))
	return nil
}
