package aggregate

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/collector/client"
)

type partitionPerfCounter struct {
	name  string
	gpid  base.Gpid
	value float64
}

// decodePartitionPerfCounter implements the v1 version of metric decoding.
func decodePartitionPerfCounter(pc *client.PerfCounter) (*partitionPerfCounter, error) {
	idx := strings.Index(pc.Name, "@")
	gpidStr := pc.Name[idx+1:]
	appIDAndPartitionID := strings.Split(gpidStr, ".")
	if len(appIDAndPartitionID) != 2 {
		return nil, fmt.Errorf("invalid perf-counter \"%s\"", pc.Name)
	}
	appID, err := strconv.Atoi(appIDAndPartitionID[0])
	if err != nil {
		return nil, fmt.Errorf("invalid AppID from perf-counter \"%s\": %s", pc.Name, err)
	}
	partitionIndex, err := strconv.Atoi(appIDAndPartitionID[1])
	if err != nil {
		return nil, fmt.Errorf("invalid PartitionIndex from perf-counter \"%s\": %s", pc.Name, err)
	}
	return &partitionPerfCounter{
		name: pc.Name[:idx], // strip out the replica id
		gpid: base.Gpid{
			Appid:          int32(appID),
			PartitionIndex: int32(partitionIndex),
		},
		value: pc.Value,
	}, nil
}

// TODO(wutao1): implement the v2 version of metric decoding according to
// https://github.com/apache/incubator-pegasus/blob/master/rfcs/2020-08-27-metric-api.md
