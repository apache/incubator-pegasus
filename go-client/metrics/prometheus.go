/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
	registry    prometheus.Registerer
	counterMap  sync.Map
	summaryMap  sync.Map
	constLabels prometheus.Labels
}

var (
	singletonMetrics          *PrometheusMetrics
	initPrometheusMetricsOnce sync.Once
	startServerOnce           sync.Once
	constLabels               map[string]string
	initRegistry              prometheus.Registerer
)

func InitMetrics(registry prometheus.Registerer, cfg config.Config) {
	initRegistry = registry
	constLabels = cfg.PrometheusConstLabels
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

		labels := prometheus.Labels{}
		for k, v := range constLabels {
			labels[k] = v
		}
		endpoint := GetLocalHostName()
		labels["endpoint"] = endpoint

		singletonMetrics = &PrometheusMetrics{
			registry:    initRegistry,
			constLabels: labels,
			counterMap:  sync.Map{},
			summaryMap:  sync.Map{},
		}
	})
	return singletonMetrics
}

func (pm *PrometheusMetrics) GetOrCreateCounter(counterName string, extraLabels map[string]string) (prometheus.Counter, error) {
	var varLabelKeys []string
	if extraLabels != nil {
		for k := range extraLabels {
			varLabelKeys = append(varLabelKeys, k)
		}
	}
	sort.Strings(varLabelKeys)
	key := fmt.Sprintf("%s-%s-%s", counterName, mapToString(pm.constLabels), strings.Join(varLabelKeys, ","))

	type onceCounterVec struct {
		once sync.Once
		vec  *prometheus.CounterVec
	}

	val, _ := pm.counterMap.LoadOrStore(key, &onceCounterVec{})
	wrapper := val.(*onceCounterVec)

	wrapper.once.Do(func() {
		wrapper.vec = promauto.With(pm.registry).NewCounterVec(
			prometheus.CounterOpts{
				Name:        counterName,
				Help:        fmt.Sprintf("Total count of Pegasus client operations. Labels: op(operation type), status(result: success/fail/timeout), table(target table name), meta(meta server address)"),
				ConstLabels: pm.constLabels,
			},
			varLabelKeys,
		)
	})

	return wrapper.vec.GetMetricWith(extraLabels)
}

func (pm *PrometheusMetrics) MarkMeter(counter prometheus.Counter, count int64) {
	counter.Add(float64(count))
}

func (pm *PrometheusMetrics) GetOrCreateSummary(summaryName string, extraLabels map[string]string) (prometheus.Observer, error) {
	var varLabelKeys []string
	if extraLabels != nil {
		for k := range extraLabels {
			varLabelKeys = append(varLabelKeys, k)
		}
	}
	sort.Strings(varLabelKeys)

	key := fmt.Sprintf("%s-%s-%s", summaryName, mapToString(pm.constLabels), strings.Join(varLabelKeys, ","))

	type onceSummary struct {
		once       sync.Once
		summaryVec *prometheus.SummaryVec
	}

	val, _ := pm.summaryMap.LoadOrStore(key, &onceSummary{})
	wrapper := val.(*onceSummary)

	wrapper.once.Do(func() {
		summaryVec := promauto.With(pm.registry).NewSummaryVec(
			prometheus.SummaryOpts{
				Name:        summaryName,
				Help:        fmt.Sprintf("Summary of Pegasus client operation latency. Labels: op(operation type), status(result: success/fail/timeout), table(target table name), meta(meta server address)"),
				ConstLabels: pm.constLabels,
				Objectives:  map[float64]float64{0.99: 0.001, 0.999: 0.0001},
				MaxAge:      5 * time.Minute,
				AgeBuckets:  5,
			},
			varLabelKeys,
		)

		wrapper.summaryVec = summaryVec
	})

	return wrapper.summaryVec.GetMetricWith(extraLabels)
}

func (pm *PrometheusMetrics) ObserveSummary(observer prometheus.Observer, value float64) {
	observer.Observe(value)
}

func mapToString(tags map[string]string) string {
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
