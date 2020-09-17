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
	tb, err := metaClient.GetTableInfo("temp")
	assert.Nil(t, err)
	assert.Equal(t, tb.AppID, 2)
	assert.Equal(t, tb.PartitionCount, 8)
}
