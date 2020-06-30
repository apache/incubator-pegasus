package client

import (
	"github.com/XiaoMi/pegasus-go-client/session"
)

// RemoteCmdClient is a client to call remote command to a Pegasus node.
type RemoteCmdClient struct {
	session *session.NodeSession
}

type PerfCounter struct {
	Name  string
	Value float64
}

func NewRemoteCmdClient(addr string) *RemoteCmdClient {
	return &RemoteCmdClient{
		session: session.NewNodeSession(addr, "unknown"),
	}
}

func (*RemoteCmdClient) GetAllPerfCounters() []*PerfCounter {
	return nil
}
