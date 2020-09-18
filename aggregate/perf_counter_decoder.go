package aggregate

import (
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
func decodePartitionPerfCounter(pc *client.PerfCounter) *partitionPerfCounter {
	idx := strings.LastIndex(pc.Name, "@")
	gpidStr := pc.Name[idx+1:]
	appIDAndPartitionID := strings.Split(gpidStr, ".")
	if len(appIDAndPartitionID) != 2 {
		// special case: in some mis-desgined metrics, what follows after a '@' may not be a replica id
		return nil
	}
	appIDAndPartitionID = appIDAndPartitionID[:2] // "AppID.PartitionIndex"
	appID, err := strconv.Atoi(appIDAndPartitionID[0])
	if err != nil {
		return nil
	}
	partitionIndex, err := strconv.Atoi(appIDAndPartitionID[1])
	if err != nil {
		return nil
	}
	return &partitionPerfCounter{
		name: pc.Name[:idx], // strip out the replica id
		gpid: base.Gpid{
			Appid:          int32(appID),
			PartitionIndex: int32(partitionIndex),
		},
		value: pc.Value,
	}
}

// TODO(wutao1): implement the v2 version of metric decoding according to
// https://github.com/apache/incubator-pegasus/blob/master/rfcs/2020-08-27-metric-api.md
