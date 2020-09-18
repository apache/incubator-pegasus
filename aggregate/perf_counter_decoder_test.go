package aggregate

import (
	"strings"
	"testing"

	"github.com/pegasus-kv/collector/client"
	"github.com/stretchr/testify/assert"
)

func TestDecodePartitionPerfCounter(t *testing.T) {
	c := client.NewRemoteCmdClient("127.0.0.1:34801")
	pcs, err := c.GetPerfCounters("@")
	assert.Nil(t, err)

	var perfCounter *partitionPerfCounter
	for _, pc := range pcs {
		if strings.Contains(pc.Name, "@1.1") {
			perfCounter, err = decodePartitionPerfCounter(pc)
			assert.Nil(t, err)
			break
		}
	}

	assert.NotNil(t, perfCounter)
	assert.Equal(t, perfCounter.gpid.Appid, int32(1))
	assert.Equal(t, perfCounter.gpid.PartitionIndex, int32(1))
}
