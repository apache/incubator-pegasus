package aggregate

import (
	"testing"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/stretchr/testify/assert"
)

func TestPerfClientGetNodeStats(t *testing.T) {
	pclient := NewPerfClient([]string{"127.0.0.1:34601"})
	nodes, err := pclient.GetNodeStats("@")
	assert.Nil(t, err)
	assert.Greater(t, len(nodes), 0)
	assert.Greater(t, len(nodes[0].Stats), 0)
	for _, n := range nodes {
		assert.NotEmpty(t, n.Addr)
	}
}

func TestPerfClientGetPartitionStats(t *testing.T) {
	pclient := NewPerfClient([]string{"127.0.0.1:34601"})
	partitions, err := pclient.GetPartitionStats()
	assert.Nil(t, err)
	assert.Greater(t, len(partitions), 0)
	assert.Greater(t, len(partitions[0].Stats), 0)
	for _, p := range partitions {
		assert.NotEmpty(t, p.Addr)
		assert.NotEqual(t, p.Gpid, base.Gpid{Appid: 0, PartitionIndex: 0})
		assert.NotEmpty(t, p.Stats)
	}
}
