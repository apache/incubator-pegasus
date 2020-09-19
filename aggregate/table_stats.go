package aggregate

import (
	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/collector/client"
)

// PartitionStats is a set of metrics retrieved from this partition.
type PartitionStats struct {
	Gpid base.Gpid

	// perfCounter's name -> the value.
	Stats map[string]float64
}

func (s *PartitionStats) update(pc *partitionPerfCounter) {
	s.Stats[pc.name] = pc.value
}

// TableStats has the aggregated metrics for this table.
type TableStats struct {
	TableName  string
	Partitions map[int]*PartitionStats

	// The aggregated value of table metrics.
	// perfCounter's name -> the value.
	Stats map[string]float64
}

func newTableStats(info *client.TableInfo) *TableStats {
	tb := &TableStats{
		TableName:  info.TableName,
		Partitions: make(map[int]*PartitionStats),
		Stats:      make(map[string]float64),
	}
	for i := 0; i < info.PartitionCount; i++ {
		tb.Partitions[i] = &PartitionStats{
			Gpid:  base.Gpid{Appid: int32(info.AppID), PartitionIndex: int32(i)},
			Stats: make(map[string]float64),
		}
	}
	return tb
}

func (tb *TableStats) aggregate() {
	for _, part := range tb.Partitions {
		for name, value := range part.Stats {
			tb.Stats[name] += value
		}
	}
}
