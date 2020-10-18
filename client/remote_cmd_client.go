package client

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/session"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func init() {
	// Sets pegasus-go-client logger to logrus.
	pegalog.SetLogger(log.StandardLogger())

	// Registers RPC handler for remote command.
	session.RegisterRPCResultHandler("RPC_CLI_CLI_CALL_ACK", func() session.RpcResponseResult {
		return &RemoteCmdServiceCallCommandResult{Success: new(string)}
	})
}

// RemoteCmdClient is a client to call remote command to a Pegasus ReplicaServer.
type RemoteCmdClient struct {
	session session.NodeSession
}

// PerfCounter is a Pegasus perf-counter.
type PerfCounter struct {
	Name  string
	Value float64
}

func (p *PerfCounter) String() string {
	return fmt.Sprintf("{Name: %s, Value: %f}", p.Name, p.Value)
}

// NewRemoteCmdClient returns an instance of RemoteCmdClient.
func NewRemoteCmdClient(addr string) *RemoteCmdClient {
	return &RemoteCmdClient{
		session: session.NewNodeSession(addr, session.NodeTypeReplica),
	}
}

// GetPerfCounters retrieves all perf-counters matched with `filter` from the remote node.
func (c *RemoteCmdClient) GetPerfCounters(filter string) ([]*PerfCounter, error) {
	result, err := c.call("perf-counters-by-substr", []string{filter})
	if err != nil {
		return nil, err
	}
	resultJSON := gjson.Parse(result)
	perfCounters := resultJSON.Get("counters").Array()
	var ret []*PerfCounter
	for _, perfCounter := range perfCounters {
		ret = append(ret, &PerfCounter{
			Name:  perfCounter.Get("name").String(),
			Value: perfCounter.Get("value").Float(),
		})
	}
	return ret, nil
}

func (c *RemoteCmdClient) call(command string, arguments []string) (cmdResult string, err error) {
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	thriftArgs := &RemoteCmdServiceCallCommandArgs{
		Cmd: &Command{Cmd: command, Arguments: arguments},
	}
	res, err := c.session.CallWithGpid(ctx, &base.Gpid{}, thriftArgs, "RPC_CLI_CLI_CALL")
	if err != nil {
		return "", err
	}
	ret, _ := res.(*RemoteCmdServiceCallCommandResult)
	return ret.GetSuccess(), nil
}

// Close terminates the session to replica.
func (c *RemoteCmdClient) Close() {
	c.session.Close()
}
