package aggregate

import (
	"testing"

	"github.com/pegasus-kv/collector/client"
	"github.com/stretchr/testify/assert"
)

func TestUpdateLocalTableMap(t *testing.T) {
	ag := &tableStatsAggregator{
		metaClient: client.NewMetaClient("127.0.0.1:34601"),
		tables:     make(map[int]*TableStats),
	}
	ag.updateTableMap()
	assert.Equal(t, len(ag.tables), 2)
	assert.Equal(t, len(ag.tables[1].Partitions), 4)
	assert.Equal(t, len(ag.tables[2].Partitions), 8)

	tables := []*client.TableInfo{
		{AppID: 1, TableName: "stat", PartitionCount: 4},
		{AppID: 2, TableName: "test", PartitionCount: 8},
		{AppID: 3, TableName: "new_table", PartitionCount: 16},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 3)
	assert.Equal(t, len(ag.tables[3].Partitions), 16)

	tables = []*client.TableInfo{
		{AppID: 1, TableName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 1)
	assert.Equal(t, len(ag.tables[1].Partitions), 4)
}

func TestUpdatePartitionStats(t *testing.T) {
	ag := &tableStatsAggregator{
		tables: make(map[int]*TableStats),
	}
	tables := []*client.TableInfo{
		{AppID: 1, TableName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)

	pc := decodePartitionPerfCounter(&client.PerfCounter{Name: "replica*app.pegasus*recent.abnormal.count@1.2", Value: 100})
	assert.NotNil(t, pc)

	ag.updatePartitionStat(pc)
	assert.Contains(t, ag.tables[1].Partitions[2].Stats, pc.name)
	assert.Equal(t, ag.tables[1].Partitions[2].Stats[pc.name], float64(100))
}
