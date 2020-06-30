package client

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientListNodes(t *testing.T) {
	metaClient := NewMetaClient([]string{"127.0.0.1:34601"})
	nodes, err := metaClient.ListNodes()
	assert.Nil(t, err)
	assert.Equal(t, len(nodes), 3)
}
