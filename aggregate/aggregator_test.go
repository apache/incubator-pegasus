package aggregate

import (
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/admin"
	"github.com/stretchr/testify/assert"
)

func TestUpdateLocalTableMap(t *testing.T) {
	ag := &tableStatsAggregator{
		client: newClient("127.0.0.1:34601"),
		tables: make(map[int]*TableStats),
	}
	ag.updateTableMap()
	assert.Equal(t, len(ag.tables), 2)
	assert.Equal(t, len(ag.tables[1].Partitions), 4) // test
	assert.Equal(t, len(ag.tables[2].Partitions), 8) // stat

	tables := []*admin.AppInfo{
		{AppID: 1, AppName: "stat", PartitionCount: 4},
		{AppID: 2, AppName: "test", PartitionCount: 8},
		{AppID: 3, AppName: "new_table", PartitionCount: 16},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 3)
	assert.Equal(t, len(ag.tables[3].Partitions), 16)

	tables = []*admin.AppInfo{
		{AppID: 1, AppName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 1)
	assert.Equal(t, len(ag.tables[1].Partitions), 4)
}

func TestUpdatePartitionStats(t *testing.T) {
	ag := &tableStatsAggregator{
		tables: make(map[int]*TableStats),
	}
	tables := []*admin.AppInfo{
		{AppID: 1, AppName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)

	pc := decodePartitionPerfCounter(&PerfCounter{Name: "replica*app.pegasus*recent.abnormal.count@1.2", Value: 100})
	assert.NotNil(t, pc)

	ag.updatePartitionStat(pc)
	assert.Contains(t, ag.tables[1].Partitions[2].Stats, pc.name)
	assert.Equal(t, ag.tables[1].Partitions[2].Stats[pc.name], float64(100))
}
