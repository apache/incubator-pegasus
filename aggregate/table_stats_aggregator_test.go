package aggregate

import (
	"testing"

	"github.com/pegasus-kv/collector/client"
	"github.com/stretchr/testify/assert"
)

func TestUpdateLocalTableMap(t *testing.T) {
	ag := &tableStatsAggregator{
		metaClient: client.NewMetaClient("127.0.0.1:34601"),
		tables:     make(map[int]*tableStats),
	}
	ag.updateTableMap()
	assert.Equal(t, len(ag.tables), 2)
	assert.Equal(t, len(ag.tables[1].partitions), 4)
	assert.Equal(t, len(ag.tables[2].partitions), 8)

	tables := []*client.TableInfo{
		{AppID: 1, TableName: "stat", PartitionCount: 4},
		{AppID: 2, TableName: "test", PartitionCount: 8},
		{AppID: 3, TableName: "new_table", PartitionCount: 16},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 3)
	assert.Equal(t, len(ag.tables[3].partitions), 16)

	tables = []*client.TableInfo{
		{AppID: 1, TableName: "stat", PartitionCount: 4},
	}
	ag.doUpdateTableMap(tables)
	assert.Equal(t, len(ag.tables), 1)
	assert.Equal(t, len(ag.tables[1].partitions), 4)
}
