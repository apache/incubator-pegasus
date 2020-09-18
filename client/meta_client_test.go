package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaClientListNodes(t *testing.T) {
	metaClient := NewMetaClient("127.0.0.1:34601")
	nodes, err := metaClient.ListNodes()
	assert.Nil(t, err)
	assert.Equal(t, len(nodes), 3)
}

func TestMetaClientTableInfo(t *testing.T) {
	metaClient := NewMetaClient("127.0.0.1:34601")
	tables, err := metaClient.ListTables()
	assert.Nil(t, err)
	assert.Equal(t, len(tables), 2)
	assert.Equal(t, tables[0].AppID, 1)
	assert.Equal(t, tables[0].TableName, "stat")
}
