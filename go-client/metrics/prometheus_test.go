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
	"sync"
	"testing"

	"github.com/apache/incubator-pegasus/go-client/config"
	"github.com/prometheus/client_golang/prometheus"
)

// mockConfig is a helper to create a mock config for testing.
func mockConfig(labels map[string]string) config.Config {
	return config.Config{
		PrometheusConstLabels: labels,
	}
}

func TestGetPrometheusMetrics_Singleton(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := mockConfig(map[string]string{"test": "label"})
	InitMetrics(reg, cfg)

	m1 := GetPrometheusMetrics()
	m2 := GetPrometheusMetrics()

	if m1 != m2 {
		t.Errorf("Expected singleton instance, but got different instances")
	}
}

func TestMarkMeter_Concurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := mockConfig(map[string]string{"test": "label"})
	InitMetrics(reg, cfg)

	pm := GetPrometheusMetrics()

	const counterName = "test_counter"
	var wg sync.WaitGroup
	const goroutines = 1000

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			pm.MarkMeter(counterName, 1, map[string]string{"test": "label1"})
			pm.MarkMeter(counterName, 1, map[string]string{"test": "label2"})
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
		if counterValue != float64(goroutines*2) {
			t.Errorf("Expected counter value %d, got %f", goroutines*2, counterValue)
		}
	}
}

func TestObserveSummary_Concurrent(t *testing.T) {
	reg := prometheus.NewRegistry()
	cfg := mockConfig(map[string]string{"test": "label"})
	InitMetrics(reg, cfg)

	pm := GetPrometheusMetrics()

	const summaryName = "test_summary"
	var wg sync.WaitGroup
	const goroutines = 1000

	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(val float64) {
			defer wg.Done()
			pm.ObserveSummary(summaryName, val, map[string]string{"test": "label1"})
			pm.ObserveSummary(summaryName, val, map[string]string{"test": "label2"})
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
		if summaryValue != float64(goroutines*(goroutines-1)) {
			t.Errorf("Expected summary value %f, got %f", float64(goroutines*(goroutines-1)), summaryValue)
		}
	}
}
