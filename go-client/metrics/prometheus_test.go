package metrics

import (
	"sync"
	"testing"

	"github.com/apache/incubator-pegasus/go-client/config"
	"github.com/prometheus/client_golang/prometheus"
)

// mockConfig is a helper to create a mock config for testing.
func mockConfig(tags map[string]string) config.Config {
	return config.Config{
		PerfCounterTags: tags,
	}
}

func TestGetPrometheusMetrics_Singleton(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := mockConfig(map[string]string{"test": "tag"})
	InitMetrics(reg, cfg)

	m1 := GetPrometheusMetrics()
	m2 := GetPrometheusMetrics()

	if m1 != m2 {
		t.Errorf("Expected singleton instance, but got different instances")
	}
}

func TestMarkMeter_Concurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := mockConfig(map[string]string{"test": "tag"})
	InitMetrics(reg, cfg)

	pm := GetPrometheusMetrics()

	const counterName = "test_counter"
	var wg sync.WaitGroup
	const goroutines = 1000

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			pm.MarkMeter(counterName, 1)
		}()
	}

	wg.Wait()

	metrics, err := reg.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}

	for _, mf := range metrics {
		var counterValue float64
		for _, m := range mf.GetMetric() {
			counterValue += m.GetCounter().GetValue()
		}
		if counterValue != float64(goroutines) {
			t.Errorf("Expected counter value %d, got %f", goroutines, counterValue)
		}
	}
}

func TestObserveSummary_Concurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := mockConfig(map[string]string{"test": "tag"})
	InitMetrics(reg, cfg)

	pm := GetPrometheusMetrics()

	const summaryName = "test_summary"
	var wg sync.WaitGroup
	const goroutines = 1000

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(val float64) {
			defer wg.Done()
			pm.ObserveSummary(summaryName, val)
		}(float64(i))
	}

	wg.Wait()

	metrics, err := reg.Gather()
	if err != nil {
		t.Errorf("Failed to gather metrics: %v", err)
	}

	for _, mf := range metrics {
		var summaryValue float64
		for _, m := range mf.GetMetric() {
			summaryValue += m.GetSummary().GetSampleSum()
		}
		if summaryValue != float64(goroutines*(goroutines-1)/2) {
			t.Errorf("Expected summary value %d, got %f", goroutines*(goroutines-1)/2, summaryValue)
		}
	}
}
