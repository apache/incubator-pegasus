package metrics

import (
	"fmt"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/incubator-pegasus/go-client/config"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// PrometheusMetrics is the metrics implementation for Prometheus.
type PrometheusMetrics struct {
	tags       map[string]string
	registry   prometheus.Registerer
	counterMap sync.Map
	summaryMap sync.Map
}

var (
	singletonMetrics          *PrometheusMetrics
	initPrometheusMetricsOnce sync.Once
	startServerOnce           sync.Once
	perfCounterMap            map[string]string
	initRegistry              prometheus.Registerer
)

func InitMetrics(registry prometheus.Registerer, cfg config.Config) {
	initRegistry = registry
	perfCounterMap = cfg.PerfCounterTags
	if cfg.EnablePrometheus {
		startServerOnce.Do(func() {
			port := 9090
			if cfg.PrometheusPort > 0 {
				port = cfg.PrometheusPort
			}
			go func() {
				http.Handle("/metrics", promhttp.Handler())
				addr := fmt.Sprintf(":%d", port)
				pegalog.GetLogger().Print("Starting Prometheus metrics server on", addr)
				if err := http.ListenAndServe(addr, nil); err != nil {
					pegalog.GetLogger().Fatal("Failed to start Prometheus metrics server:", err)
				}
			}()
		})
	}
}

// GetPrometheusMetrics get singleton PrometheusMetrics
func GetPrometheusMetrics() *PrometheusMetrics {
	initPrometheusMetricsOnce.Do(func() {
		if initRegistry == nil {
			initRegistry = prometheus.DefaultRegisterer
		}

		tags := make(map[string]string)

		for k, v := range perfCounterMap {
			tags[k] = v
		}
		endpoint := GetLocalHostName()
		tags["endpoint"] = endpoint

		singletonMetrics = &PrometheusMetrics{
			registry:   initRegistry,
			tags:       tags,
			counterMap: sync.Map{},
			summaryMap: sync.Map{},
		}
	})
	return singletonMetrics
}

func (pm *PrometheusMetrics) MarkMeter(counterName string, count int64) {
	key := fmt.Sprintf("%s-%s", counterName, tagsToString(pm.tags))

	type onceCounter struct {
		once    sync.Once
		counter prometheus.Counter
	}

	val, _ := pm.counterMap.LoadOrStore(key, &onceCounter{})
	wrapper := val.(*onceCounter)

	wrapper.once.Do(func() {
		labels := prometheus.Labels{}
		for k, v := range pm.tags {
			labels[k] = v
		}

		counter := promauto.With(pm.registry).NewCounter(prometheus.CounterOpts{
			Name:        counterName,
			Help:        fmt.Sprintf("Counter for %s", counterName),
			ConstLabels: labels,
		})

		wrapper.counter = counter
	})

	wrapper.counter.Add(float64(count))
}

func (pm *PrometheusMetrics) ObserveSummary(summaryName string, value float64) {
	key := fmt.Sprintf("%s-%s", summaryName, tagsToString(pm.tags))

	type onceSummary struct {
		once    sync.Once
		summary prometheus.Summary
	}

	val, _ := pm.summaryMap.LoadOrStore(key, &onceSummary{})
	wrapper := val.(*onceSummary)

	wrapper.once.Do(func() {
		labels := prometheus.Labels{}
		for k, v := range pm.tags {
			labels[k] = v
		}

		summary := promauto.With(pm.registry).NewSummary(prometheus.SummaryOpts{
			Name:        summaryName,
			Help:        fmt.Sprintf("Summary for %s", summaryName),
			ConstLabels: labels,
			Objectives:  map[float64]float64{0.99: 0.001, 0.999: 0.0001},
			MaxAge:      5 * time.Minute,
			AgeBuckets:  5,
		})

		wrapper.summary = summary
	})

	wrapper.summary.Observe(value)
}

func tagsToString(tags map[string]string) string {
	var keys []string
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%s=%s", k, tags[k]))
	}
	return sb.String()
}

func GetLocalHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return hostname
}
