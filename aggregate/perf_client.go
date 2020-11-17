package aggregate

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/admin"
	"github.com/XiaoMi/pegasus-go-client/session"
	"github.com/tidwall/gjson"
)

// PerfClient is a client to get perf-counters from a Pegasus ReplicaServer.
type PerfClient struct {
	*admin.RemoteCmdClient
}

// PerfCounter is a Pegasus perf-counter.
type PerfCounter struct {
	Name  string
	Value float64
}

func (p *PerfCounter) String() string {
	return fmt.Sprintf("{Name: %s, Value: %f}", p.Name, p.Value)
}

// NewPerfClient returns an instance of PerfClient.
func NewPerfClient(addr string) *PerfClient {
	return &PerfClient{
		RemoteCmdClient: admin.NewRemoteCmdClient(addr, session.NodeTypeReplica),
	}
}

// GetPerfCounters retrieves all perf-counters matched with `filter` from the remote node.
func (c *PerfClient) GetPerfCounters(filter string) ([]*PerfCounter, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	result, err := c.Call(ctx, "perf-counters-by-substr", []string{filter})
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

// Close terminates the session to replica.
func (c *PerfClient) Close() {
	/*TODO*/
}
