package metrics

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
)

// FalconMetric is the metric data structure for Falcon.
type FalconMetric struct {
	Endpoint    string  `json:"endpoint"`
	Metric      string  `json:"metric"`
	Timestamp   int64   `json:"timestamp"`
	Step        int     `json:"step"`
	Value       float64 `json:"value"`
	CounterType string  `json:"counterType"`
	Tags        string  `json:"tags"`
}

// FalconReporter does not collect metrics directly. Instead, it fetches metrics
// from the Prometheus registry and reports them to the Falcon server.
type FalconReporter struct {
	FalconServer    string
	ReportInterval  time.Duration
	Registry        prometheus.Gatherer
	lastCounterData map[interface{}]float64
	logger          pegalog.Logger
}

var (
	singletonFalconReporter *FalconReporter
	falconOnce              sync.Once
)

func GetFalconReporter(endpoint string, interval int) *FalconReporter {
	falconOnce.Do(func() {
		if gatherer, ok := initRegistry.(prometheus.Gatherer); ok {
			u, err := url.Parse(endpoint)
			if err != nil {
				pegalog.GetLogger().Fatal("Failed to parse Falcon endpoint: %v", err)
			}
			u.Path = "/v1/push"
			endpoint = u.String()
			singletonFalconReporter = &FalconReporter{
				FalconServer:    endpoint,
				ReportInterval:  time.Duration(interval) * time.Second,
				Registry:        gatherer,
				lastCounterData: make(map[interface{}]float64),
				logger:          pegalog.GetLogger(),
			}
		} else {
			pegalog.GetLogger().Printf("initRegistry is not a prometheus.Gatherer")
		}
	})
	return singletonFalconReporter
}

// Start initiates the periodic metric reporting process.
func (r *FalconReporter) Start() {
	ticker := time.NewTicker(r.ReportInterval)
	defer ticker.Stop()

	for range ticker.C {
		if err := r.reportOnce(); err != nil {
			r.logger.Printf("Falcon report failed: %v", err)
		}
	}
}

// reportOnce gathers metrics from the prometheus registry and reports them to the Falcon server.
func (r *FalconReporter) reportOnce() error {
	metrics, err := r.Registry.Gather()
	if err != nil {
		r.logger.Printf("Failed to gather metrics: %v", err)
		return err
	}

	var falconMetrics []FalconMetric
	timestamp := time.Now().Unix()
	step := int(r.ReportInterval.Seconds())

	for _, mf := range metrics {
		metricName := convertPromNameToFalcon(mf.GetName())
		for _, m := range mf.GetMetric() {
			key := getMetricUniqueKey(mf.GetName(), m)
			switch mf.GetType() {
			case io_prometheus_client.MetricType_COUNTER:
				currentValue := m.GetCounter().GetValue()
				lastValue, loaded := r.lastCounterData[key]
				var delta float64
				if loaded {
					delta = currentValue - lastValue
				} else {
					delta = currentValue
				}
				r.lastCounterData[key] = currentValue
				falconMetric := genFalconMetric(metricName, m, delta, timestamp, step, "GAUGE")
				falconMetrics = append(falconMetrics, falconMetric)

			case io_prometheus_client.MetricType_SUMMARY:
				summary := m.GetSummary()
				if summary == nil {
					continue
				}
				var p99, p999 float64
				for _, q := range summary.Quantile {
					if q.GetQuantile() == 0.99 {
						p99 = q.GetValue()
					} else if q.GetQuantile() == 0.999 {
						p999 = q.GetValue()
					}
				}
				p99MetricName := fmt.Sprintf("%s.p99", metricName)
				p99FalconMetric := genFalconMetric(p99MetricName, m, p99, timestamp, step, "GAUGE")
				falconMetrics = append(falconMetrics, p99FalconMetric)

				p999MetricName := fmt.Sprintf("%s.p999", metricName)
				p999FalconMetric := genFalconMetric(p999MetricName, m, p999, timestamp, step, "GAUGE")
				falconMetrics = append(falconMetrics, p999FalconMetric)
			default:
				continue
			}
		}
	}

	if len(falconMetrics) == 0 {
		return nil
	}

	payload, err := json.Marshal(falconMetrics)
	if err != nil {
		r.logger.Printf("Failed to marshal metrics: %v", err)
		return err
	}

	req, err := http.NewRequest("POST", r.FalconServer, bytes.NewBuffer(payload))
	if err != nil {
		r.logger.Printf("Failed to create request: %v", err)
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Connection", "keep-alive")
	req.ContentLength = int64(len(payload))

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		r.logger.Printf("Failed to post metrics to Falcon: %v", err)
		return err
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			r.logger.Printf("Failed to close response body: %v", err)
		}
	}()

	if resp.StatusCode != http.StatusOK {
		r.logger.Printf("Falcon endpoint returned non-200 status code: %s", resp.Status)
		return err
	}

	return nil
}

func genFalconMetric(metricName string, m *io_prometheus_client.Metric, value float64, timestamp int64, step int, counterType string) FalconMetric {
	tags := make([]string, 0, len(m.Label))
	for _, label := range m.Label {
		tags = append(tags, fmt.Sprintf("%s=%s", *label.Name, *label.Value))
	}
	return FalconMetric{
		Endpoint:    GetLocalHostName(),
		Metric:      metricName,
		Timestamp:   timestamp,
		Step:        step,
		Value:       value,
		CounterType: counterType,
		Tags:        strings.Join(tags, ","),
	}
}

func getMetricUniqueKey(name string, m *io_prometheus_client.Metric) interface{} {
	var key strings.Builder
	key.WriteString(name)
	for _, label := range m.Label {
		key.WriteString(fmt.Sprintf("%s=%s", *label.Name, *label.Value))
	}
	return key.String()
}

func convertPromNameToFalcon(promName string) string {
	return strings.ReplaceAll(promName, "_", ".")
}
