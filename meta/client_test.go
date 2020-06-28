package meta

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestClientListNodes(t *testing.T) {
	metaClient := NewClient()
	nodes, err := metaClient.ListNodes()
	assert.Nil(t, err)
	assert.Equal(t, len(nodes), 3)
}
