package executor

import (
	"encoding/json"
	"fmt"
	"github.com/XiaoMi/pegasus-go-client/session"
)

var PerfCounters = "perf-counters"
var PerfCountersBySubstr = "perf-counters-by-substr"
var PerfCountersByPrefix = "perf-counters-by-prefix"
var PerfCountersByPostfix = "perf-counters-by-postfix"

type PerfCounterResult struct {
	Name  string  `json:"name"`
	Type  string  `json:"type"`
	Value float64 `json:"value"`
}

type RemoteCmdResponse struct {
	Result    string              `json:"result"`
	Timestamp string              `json:"timestamp"`
	Date      string              `json:"timestamp_str"`
	Counters  []PerfCounterResult `json:"counters"`
}

func generateReplicaCounter(counter string, gpid string) string {
	return fmt.Sprintf("replica.*%s@%s", counter, gpid)
}

func GetReplicaCounter(client *Client, addr string, counter string, gpid string) float64 {
	resp, err := SendRemoteCommand(client, session.NodeTypeReplica, addr, PerfCounters, []string{generateReplicaCounter(counter, gpid)})
	if err != nil {
		return 0.0
	}

	var remoteCmdResponse RemoteCmdResponse
	_ = json.Unmarshal([]byte(resp), &remoteCmdResponse)

	if len(remoteCmdResponse.Counters) == 1 {
		return remoteCmdResponse.Counters[0].Value
	}

	return 0.0
}
