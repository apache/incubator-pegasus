package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/apache/incubator-pegasus/go-client/config"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFalconReporter_ReportOnce(t *testing.T) {
	registry := prometheus.NewRegistry()
	mockFalconServer := newMockFalconServer(t)
	defer mockFalconServer.Close()

	InitMetrics(registry, config.Config{})
	pm := GetPrometheusMetrics()
	reporter := GetFalconReporter(mockFalconServer.URL, 10)
	reporter.logger = pegalog.GetLogger()

	t.Run("test with empty registry", func(t *testing.T) {
		err := reporter.reportOnce()
		assert.NoError(t, err)
		require.Len(t, mockFalconServer.receivedMetrics, 0)
	})

	t.Run("test successful multiple reports", func(t *testing.T) {
		// 1st report: 1-10000 values, counter=100
		for i := 1; i <= 10000; i++ {
			pm.ObserveSummary("test_summary", float64(i))
		}
		pm.MarkMeter("test_counter", 100)
		assert.NoError(t, reporter.reportOnce())

		time.Sleep(200 * time.Millisecond)

		// 2nd report: 10001-20000 values, counter=200
		for i := 10001; i <= 20000; i++ {
			pm.ObserveSummary("test_summary", float64(i))
		}
		pm.MarkMeter("test_counter", 200)
		assert.NoError(t, reporter.reportOnce())

		time.Sleep(200 * time.Millisecond)

		// 3rd report: 20001-30000 values, counter=300
		for i := 20001; i <= 30000; i++ {
			pm.ObserveSummary("test_summary", float64(i))
		}
		pm.MarkMeter("test_counter", 300)
		assert.NoError(t, reporter.reportOnce())

		require.Len(t, mockFalconServer.receivedMetrics, 3)
		validateCounterMetrics(t, mockFalconServer.receivedMetrics)
		validateSummaryMetrics(t, mockFalconServer.receivedMetrics)
	})
}

func validateCounterMetrics(t *testing.T, allReports [][]FalconMetric) {
	var counterReports []FalconMetric
	for _, report := range allReports {
		for _, m := range report {
			if m.Metric == "test.counter" {
				counterReports = append(counterReports, m)
			}
		}
	}

	require.Len(t, counterReports, 3)
	// Counter deltas should be 100, 200, 300 (each added once per report)
	assert.Equal(t, float64(100), counterReports[0].Value, "1st counter delta mismatch")
	assert.Equal(t, float64(200), counterReports[1].Value, "2nd counter delta mismatch")
	assert.Equal(t, float64(300), counterReports[2].Value, "3rd counter delta mismatch")
}

func validateSummaryMetrics(t *testing.T, allReports [][]FalconMetric) {
	require.Len(t, allReports, 3)

	expected := []struct {
		p99  float64
		p999 float64
	}{
		{9900, 9990},   // 1 - 10000
		{19800, 19980}, // 1 - 20000
		{29700, 29970}, // 1 - 30000
	}

	for i, report := range allReports {
		var p99, p999 float64
		for _, m := range report {
			switch m.Metric {
			case "test.summary.p99":
				p99 = m.Value
			case "test.summary.p999":
				p999 = m.Value
			}
		}
		// Allow minor error margin for Prometheus summary approximation
		assert.InDelta(t, expected[i].p99, p99, float64((i+1)*10), "p99 mismatch in report %d", i+1)
		assert.InDelta(t, expected[i].p999, p999, float64((i+1)*10), "p999 mismatch in report %d", i+1)
	}
}

type mockFalconServer struct {
	*httptest.Server
	receivedMetrics [][]FalconMetric
	mu              sync.Mutex
}

func newMockFalconServer(t *testing.T) *mockFalconServer {
	s := &mockFalconServer{receivedMetrics: make([][]FalconMetric, 0)}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()

		var metrics []FalconMetric
		if err := json.NewDecoder(r.Body).Decode(&metrics); err != nil {
			t.Errorf("Failed to decode Falcon metrics: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		s.mu.Lock()
		s.receivedMetrics = append(s.receivedMetrics, metrics)
		s.mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	return s
}
