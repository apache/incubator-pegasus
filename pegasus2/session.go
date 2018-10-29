package pegasus2

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/rpc"
	"github.com/XiaoMi/pegasus-go-client/session"
)

type nodeSession struct {
	logger pegalog.Logger

	addr  string
	ntype session.NodeType
	conn  *rpc.RpcConn
	codec rpc.Codec
	seqId int32

	// mutex lock exclusive for CallWithGpid
	callMut sync.Mutex
}

func newNodeSession(addr string, ntype session.NodeType) session.NodeSession {
	logger := pegalog.GetLogger()

	n := &nodeSession{
		logger: pegalog.GetLogger(),
		ntype:  ntype,
		codec:  session.NewPegasusCodec(),
		addr:   addr,
		seqId:  0,
		conn:   rpc.NewRpcConn(addr),
	}
	logger.Printf("create session with %s", n)
	return n
}

func (n *nodeSession) String() string {
	return fmt.Sprintf("[%s(%s)]", n.addr, n.ntype)
}

func (n *nodeSession) CallWithGpid(ctx context.Context, gpid *base.Gpid, args session.RpcRequestArgs, name string) (session.RpcResponseResult, error) {
	// ensure CallWithGpid not being called concurrently
	n.callMut.Lock()
	defer n.callMut.Unlock()

	if n.conn.GetState() != rpc.ConnStateReady {
		if err := n.conn.TryConnect(); err != nil {
			return nil, err
		}
	}

	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		timeout := deadline.Sub(time.Now())
		if timeout > time.Duration(0) {
			n.conn.SetWriteTimeout(timeout)
		} else {
			return nil, errors.New("send rpc timeout")
		}
	}

	{ // send request
		n.seqId++
		rcall, err := session.MarshallPegasusRpc(n.codec, n.seqId, gpid, args, name)
		if err != nil {
			return nil, err
		}
		if err = n.conn.Write(rcall.RawReq); err != nil {
			return nil, err
		}
	}

	for { // read response
		if hasDeadline {
			timeout := deadline.Sub(time.Now())
			if timeout > time.Duration(0) {
				n.conn.SetReadTimeout(timeout)
			}
		}

		rcallRecv, err := session.ReadRpcResponse(n.conn, n.codec)
		if err != nil {
			return nil, err
		}
		if rcallRecv.Err != nil {
			return nil, rcallRecv.Err
		}
		if n.seqId != rcallRecv.SeqId {
			n.logger.Printf("ignore stale response (seqId: %d, current seqId: %d) from %s: %s",
				rcallRecv.SeqId, n.seqId, n, rcallRecv.Name)
			continue
		}
		return rcallRecv.Result, nil
	}
}

func (n *nodeSession) ConnState() rpc.ConnState {
	return n.conn.GetState()
}

func (n *nodeSession) Close() error {
	return n.conn.Close()
}
