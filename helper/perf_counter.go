package helper

import (
	"fmt"
	"github.com/pegasus-kv/collector/aggregate"
)

func GetReplicaCounterValue(perfClient *aggregate.PerfSession, counter string, gpid string) int64 {
	counters, err := perfClient.GetPerfCounters(fmt.Sprintf("%s@%s", counter, gpid))
	if err != nil {
		return -1
	}

	if len(counters) == 1 {
		return int64(counters[0].Value)
	}
	return -1
}

func GetNodeCounterValue(perfClient *aggregate.PerfSession, counter string) int64 {
	counters, err := perfClient.GetPerfCounters(fmt.Sprintf("%s", counter))
	if err != nil {
		return -1
	}

	if len(counters) == 1 {
		return int64(counters[0].Value)
	}
	return -1
}

func GetNodeAggregateCounterValue(perfClient *aggregate.PerfSession, counter string) int64 {
	counters, err := perfClient.GetPerfCounters(fmt.Sprintf("%s", counter))
	if err != nil {
		return -1
	}

	var value float64
	for _, counter := range counters {
		value += counter.Value
	}
	return int64(value)
}
