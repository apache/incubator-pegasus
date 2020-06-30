package client

import (
	"context"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/tidwall/gjson"
)

// RemoteCmdClient is a client to call remote command to a Pegasus ReplicaServer.
type RemoteCmdClient struct {
	session session.NodeSession
}

// PerfCounter is a Pegasus perf-counter.
type PerfCounter struct {
	Name  string
	Value float64
}

// NewRemoteCmdClient returns an instance of RemoteCmdClient.
func NewRemoteCmdClient(addr string) *RemoteCmdClient {
	return &RemoteCmdClient{
		session: session.NewNodeSession(addr, session.NodeTypeReplica),
	}
}

// GetAllPerfCounters retrieves all perf-counters from the remote node.
func (c *RemoteCmdClient) GetAllPerfCounters() ([]*PerfCounter, error) {
	result, err := c.call("perf-counters", []string{".*@.*"})
	if err != nil {
		return nil, err
	}
	perfCounters := gjson.Parse(result).Get("counters").Array()
	var ret []*PerfCounter
	for _, perfCounter := range perfCounters {
		ret = append(ret, &PerfCounter{
			Name:  perfCounter.Get("name").String(),
			Value: perfCounter.Get("value").Float(),
		})
	}
	return ret, nil
}

func (c *RemoteCmdClient) call(cmd string, args []string) (result string, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	resp, err := c.session.CallWithGpid(ctx, &base.Gpid{}, &Command{Cmd: cmd, Arguments: args}, "RPC_CLI_CLI_CALL")
	if err != nil {
		return "", err
	}
	ret, _ := resp.(*RrdbGetResult)
	return ret.GetSuccess(), nil
}
