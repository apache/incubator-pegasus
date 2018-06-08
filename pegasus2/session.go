package pegasus2

import (
	"context"
	"fmt"
	"sync"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/rpc"
	"github.com/XiaoMi/pegasus-go-client/session"
	"time"
)

type nodeSession struct {
	logger pegalog.Logger

	addr  string
	ntype session.NodeType
	conn  *rpc.RpcConn
	codec rpc.Codec
	seqId int32

	mu sync.Mutex
}

func newNodeSession(addr string, ntype session.NodeType) session.NodeSession {
	logger := pegalog.GetLogger()

	n := &nodeSession{
		logger: pegalog.GetLogger(),
		ntype:  ntype,
		codec:  &session.PegasusCodec{},
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
	n.mu.Lock()
	defer n.mu.Unlock()

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
			n.conn.SetReadTimeout(timeout)
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

	{ // read response
		rcallRecv, err := session.ReadRpcResponse(n.conn, n.codec)
		if err != nil {
			return nil, err
		}
		if rcallRecv.Err != nil {
			return nil, rcallRecv.Err
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
