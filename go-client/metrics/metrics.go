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
	"github.com/apache/incubator-pegasus/go-client/config"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Summary struct {
	summary *prometheus.SummaryVec
}

var (
	once                           sync.Once
	constLabels                    map[string]string
	PegasusClientOperationsSummary *Summary
	PegasusClientRpcSizeSummary    *Summary
)

func InitMetrics(cfg config.Config) {
	constLabels = cfg.PrometheusConstLabels
	constLabels["endpoint"] = GetLocalHostName()

	once.Do(func() {
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

		PegasusClientOperationsSummary = RegisterPromSummary("pegasus_client_operations", []string{"table", "operation", "status", "meta_addresses"})
		PegasusClientRpcSizeSummary = RegisterPromSummary("pegasus_client_rpc_size", []string{"type"})
	})
}

func RegisterPromSummary(summaryName string, extraLabels []string) *Summary {
	summaryVec := prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:        summaryName,
			ConstLabels: constLabels,
			Objectives:  map[float64]float64{0.99: 0.001, 0.999: 0.0001},
			MaxAge:      5 * time.Minute,
			AgeBuckets:  5,
		},
		extraLabels,
	)

	return &Summary{
		summary: summaryVec,
	}
}

func (s *Summary) Observe(extraLabelsVals []string, value float64) {
	s.summary.WithLabelValues(extraLabelsVals...).Observe(value)
}

func GetLocalHostName() string {
	hostname, err := os.Hostname()
	if err != nil {
		return ""
	}
	return hostname
}
