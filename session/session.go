// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/rpc"
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

	reqc        chan *requestListener
	pendingResp map[int32]*requestListener
	mu          sync.Mutex

	redialc      chan bool
	lastDialTime time.Time

	codec rpc.Codec
}

type requestListener struct {
	ch   chan bool
	call *rpcCall
}

type nodeType string

const (
	kNodeTypeMeta    nodeType = "meta"
	kNodeTypeReplica          = "replica"

	kDialInterval = time.Second * 60
)

func newNodeSessionAddr(addr string, ntype nodeType) *nodeSession {
	return &nodeSession{
		logger:      pegalog.GetLogger(),
		ntype:       ntype,
		seqId:       0,
		codec:       &PegasusCodec{},
		pendingResp: make(map[int32]*requestListener),
		reqc:        make(chan *requestListener),
		addr:        addr,
		tom:         &tomb.Tomb{},

		//
		redialc: make(chan bool, 1),
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

// thread-safe
func (n *nodeSession) ConnState() rpc.ConnState {
	return n.conn.GetState()
}

func (n *nodeSession) String() string {
	return fmt.Sprintf("addr: %s, nodetype: %s, pending rpc: %d", n.addr, n.ntype, len(n.pendingResp))
}

// Close the session if it's manually closed by user, otherwise it will
// restart dialing since the loop may end due to some io errors, and
// we need to wait until the connection recovers.
func (n *nodeSession) closeOrRestartIfLoopEnded(loop func() error) error {
	err := loop()

	if n.ConnState() == rpc.ConnStateClosed {
		n.Close()
		return err
	} else {
		n.tryDial()
		return nil
	}
}

// Loop in background and keep watching for redialc.
// Since loopForDialing is the only consumer of redialc, it guarantees
// only 1 goroutine dialing simultaneously.
func (n *nodeSession) loopForDialing() error {
	// the first try never fails.
	n.tryDial()

	for {
		select {
		case <-n.tom.Dying():
			return n.tom.Err()
		case <-n.redialc:
			if n.ConnState() != rpc.ConnStateReady {
				n.dial()
			}
		}
	}
}

func (n *nodeSession) tryDial() {
	select {
	case n.redialc <- true:
	default:
	}
}

// If the dialing ended successfully, it will start loopForRequest and
// loopForResponse which handle the data communications.
// If the last attempt failed, it will retry again.
func (n *nodeSession) dial() {
	if time.Now().Sub(n.lastDialTime) < kDialInterval {
		select {
		case <-time.After(kDialInterval):
		case <-n.tom.Dying():
			return
		}
	}

	select {
	case <-n.tom.Dying():
		// ended if session closed.
	default:
		err := n.conn.TryConnect()
		n.lastDialTime = time.Now()

		if err != nil {
			n.logger.Printf("failed to dial %s: %s", n.ntype, err)
			n.tryDial()
		} else {
			n.tom.Go(func() error {
				return n.closeOrRestartIfLoopEnded(n.loopForRequest)
			})
			n.tom.Go(func() error {
				return n.closeOrRestartIfLoopEnded(n.loopForResponse)
			})
		}
	}

	n.logger.Printf("stop dialing for %s: %s, connection state: %s", n.ntype, n.addr, n.ConnState())
}

func (n *nodeSession) notifyCallerAndDrop(req *requestListener) {
	select {
	// notify the caller
	case req.ch <- true:
		n.mu.Lock()
		delete(n.pendingResp, req.call.seqId)
		n.mu.Unlock()
	default:
		panic("impossible for concurrent notifiers")
	}
}

// single-routine worker used for sending requests.
// Any un-retryable error occurred will end up this goroutine.
func (n *nodeSession) loopForRequest() error {
	for {
		select {
		case <-n.tom.Dying():
			return n.tom.Err()
		case req := <-n.reqc:
			n.mu.Lock()
			n.pendingResp[req.call.seqId] = req
			n.mu.Unlock()

			if err := n.writeRequest(req.call); err != nil {
				n.logger.Printf("failed to send request to [%s, %s]: %s", n.addr, n.ntype, err)

				// notify the rpc caller.
				req.call.err = err
				n.notifyCallerAndDrop(req)

				// don give up if there's still hope
				if !rpc.IsNetworkTimeoutErr(err) {
					return err
				}
			}
		}
	}
}

// single-routine worker used for reading response.
// We register a map of sequence id -> recvItem when each request comes,
// so that when a response is received, we are able to notify its caller.
// Any un-retryable error occurred will end up this goroutine.
func (n *nodeSession) loopForResponse() error {
	for {
		select {
		case <-n.tom.Dying():
			return n.tom.Err()
		default:
		}

		call, err := n.readResponse()
		if err != nil {
			if rpc.IsNetworkTimeoutErr(err) {
				select {
				case <-time.After(time.Second):
					continue
				case <-n.tom.Dying():
					return n.tom.Err()
				}
			} else {
				n.logger.Printf("failed to read response from [%s, %s]: %s", n.addr, n.ntype, err)
				return err
			}
		}

		n.mu.Lock()
		reqListener, ok := n.pendingResp[call.seqId]
		n.mu.Unlock()

		if !ok {
			n.logger.Printf("ignore stale response (seqId: %d) from [%s, %s]: %s",
				call.seqId, n.ntype, n.addr, call.result)
			continue
		}

		reqListener.call.err = call.err
		reqListener.call.result = call.result
		n.notifyCallerAndDrop(reqListener)
	}
}

// Invoke a rpc call.
// The call will be cancelled if any io error encountered.
func (n *nodeSession) callWithGpid(ctx context.Context, gpid *base.Gpid, args rpcRequestArgs, name string) (result rpcResponseResult, err error) {
	rcall := &rpcCall{}
	rcall.args = args
	rcall.name = name
	rcall.seqId = atomic.AddInt32(&n.seqId, 1) // increment sequence id
	rcall.gpid = gpid

	rcall.rawReq, err = n.codec.Marshal(rcall)
	if err != nil {
		return nil, err
	}

	req := &requestListener{call: rcall, ch: make(chan bool, 1)}

	// either the ctx cancelled or the tomb killed will stop this rpc call.
	ctxWithTomb := n.tom.Context(ctx)
	select {
	// passes the request to loopForRequest
	case n.reqc <- req:
		select {
		// receive from loopForResponse, or loopRequest failed
		case <-req.ch:
			err = rcall.err
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
	return n.conn.Write(r.rawReq)
}

// readResponse never returns nil `rpcCall` unless the tcp round trip failed.
// The pegasus node may responds with not-ERR_OK error code (together with
// sequence id and rpc name) which doesn't mean that it failed due to
// transport failure. This error should be handled by the upper-level rpc caller.
// This session will not be closed for it.
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
	if err != nil || len(buf) != int(resplen) {
		return nil, err
	}

	r := &rpcCall{}
	if err := n.codec.Unmarshal(buf, r); err != nil {
		return nil, err
	}
	return r, nil
}

func (n *nodeSession) Close() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.ConnState() != rpc.ConnStateClosed {
		n.logger.Printf("Close session with [%s, %s]", n.ntype, n.addr)
		n.conn.Close()
		n.tom.Kill(errors.New("nodeSession closed"))
	}

	return n.tom.Dead()
}
