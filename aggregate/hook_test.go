package aggregate

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestAddHook(t *testing.T) {
	var actualTableStats []TableStats
	var actualClusterStats ClusterStats
	AddHookAfterTableStatEmitted(func(stats []TableStats, allStats ClusterStats) {
		actualTableStats = stats
		actualClusterStats = allStats
	})

	t1 := TableStats{TableName: "test", Stats: map[string]float64{"write": 512.0}, Timestamp: time.Now()}
	t2 := TableStats{TableName: "stat", Stats: map[string]float64{"write": 256.0}, Timestamp: time.Now()}
	all := ClusterStats{Stats: map[string]float64{"write": 768.0}}

	ch := make(chan interface{})
	go func() {
		hooksManager.afterTablStatsEmitted([]TableStats{t1, t2}, all)
		ch <- nil
	}()
	<-ch

	assert.EqualValues(t, actualTableStats, []TableStats{t1, t2})
	assert.EqualValues(t, actualClusterStats, all)

	// clear up
	hooksManager = tableStatsHooksManager{}
}
