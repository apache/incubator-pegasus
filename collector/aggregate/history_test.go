package aggregate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHistory(t *testing.T) {
	hooksManager = tableStatsHooksManager{}
	initHistoryStore()

	for i := 0; i < historyMaxCapacity*2; i++ {
		hooksManager.afterTableStatsEmitted([]TableStats{},
			ClusterStats{Stats: map[string]float64{"write": 100.0 * float64(i)}, Timestamp: time.Now()})
	}
	clusterStats := SnapshotClusterStats()
	assert.Equal(t, len(clusterStats), historyMaxCapacity)
	for i := 0; i < historyMaxCapacity; i++ {
		assert.Equal(t, clusterStats[i].Stats["write"], float64(historyMaxCapacity+i)*100.0)
	}
}
