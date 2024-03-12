// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package metrics

import (
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tidwall/gjson"
	"gopkg.in/tomb.v2"
)

const (
	MetaServer    string = "meta"
	ReplicaServer string = "replica"
)

type Metric struct {
	name string
	// For metric type for counter/gauge.
	value float64
	// For metric type of percentile.
	values []float64
	mtype  string
}

type Metrics []Metric

var GaugeMetricsMap map[string]prometheus.GaugeVec
var CounterMetricsMap map[string]prometheus.CounterVec
var SummaryMetricsMap map[string]prometheus.Summary

var TableNameByID map[string]string

type MetricCollector interface {
	Run(tom *tomb.Tomb) error
}

func NewMetricCollector(
	role string,
	detectInterval time.Duration,
	detectTimeout time.Duration) MetricCollector {
	GaugeMetricsMap = make(map[string]prometheus.GaugeVec, 128)
	CounterMetricsMap = make(map[string]prometheus.CounterVec, 128)
	SummaryMetricsMap = make(map[string]prometheus.Summary, 128)
	TableNameByID = make(map[string]string, 128)

	var collector = Collector{detectInterval: detectInterval, detectTimeout: detectTimeout, role: role}
	collector.initMetrics()
	return &collector
}

type Collector struct {
	detectInterval time.Duration
	detectTimeout  time.Duration
	role           string
}

func (collector *Collector) Run(tom *tomb.Tomb) error {
	ticker := time.NewTicker(collector.detectInterval)
	for {
		select {
		case <-tom.Dying():
			return nil
		case <-ticker.C:
			updateClusterTableInfo()
			collector.processAllServerMetrics()
		}
	}
}

// Get replica server address.
func getReplicaAddrs() ([]string, error) {
	addrs := viper.GetStringSlice("meta_servers")
	var rserverAddrs []string
	for _, addr := range addrs {
		url := fmt.Sprintf("http://%s/meta/nodes", addr)
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode != http.StatusOK {
			err = errors.New(resp.Status)
		}
		if err != nil {
			log.Errorf("Fail to get replica server address from %s, err %s", addr, err)
			continue
		}
		body, _ := ioutil.ReadAll(resp.Body)
		jsonData := gjson.Parse(string(body))
		for key := range jsonData.Get("details").Map() {
			rserverAddrs = append(rserverAddrs, key)
		}
		defer resp.Body.Close()
		break
	}
	return rserverAddrs, nil
}

// Register all metrics.
func (collector *Collector) initMetrics() {
	var addrs []string
	var err error
	if collector.role == MetaServer {
		addrs = viper.GetStringSlice("meta_servers")
	} else {
		addrs, err = getReplicaAddrs()
		if err != nil {
			log.Errorf("Get replica server address failed, err: %s", err)
			return
		}
	}
	for _, addr := range addrs {
		data, err := getOneServerMetrics(addr)
		if err != nil {
			log.Errorf("Get raw metrics from %s failed, err: %s", addr, err)
			return
		}
		jsonData := gjson.Parse(data)
		for _, entity := range jsonData.Get("entities").Array() {
			for _, metric := range entity.Get("metrics").Array() {
				var name string = metric.Get("name").String()
				var mtype string = metric.Get("type").String()
				var desc string = metric.Get("desc").String()
				switch mtype {
				case "Counter":
					if _, ok := CounterMetricsMap[name]; ok {
						continue
					}
					counterMetric := promauto.NewCounterVec(prometheus.CounterOpts{
						Name: name,
						Help: desc,
					}, []string{"endpoint", "role", "level", "title"})
					CounterMetricsMap[name] = *counterMetric
				case "Gauge":
					if _, ok := GaugeMetricsMap[name]; ok {
						continue
					}
					gaugeMetric := promauto.NewGaugeVec(prometheus.GaugeOpts{
						Name: name,
						Help: desc,
					}, []string{"endpoint", "role", "level", "title"})
					GaugeMetricsMap[name] = *gaugeMetric
				case "Percentile":
					if _, ok := SummaryMetricsMap[name]; ok {
						continue
					}
					summaryMetric := promauto.NewSummary(prometheus.SummaryOpts{
						Name: name,
						Help: desc,
						Objectives: map[float64]float64{
							0.5: 0.05, 0.9: 0.01, 0.95: 0.005, 0.99: 0.001, 0.999: 0.0001},
					})
					SummaryMetricsMap[name] = summaryMetric
				case "Histogram":
				default:
					log.Errorf("Unsupport metric type %s", mtype)
				}
			}
		}
	}
}

// Parse metric data and update metrics.
func (collector *Collector) processAllServerMetrics() {
	var addrs []string
	var err error
	if collector.role == MetaServer {
		addrs = viper.GetStringSlice("meta_servers")
	} else {
		addrs, err = getReplicaAddrs()
		if err != nil {
			log.Errorf("Get replica server address failed, err: %s", err)
			return
		}
	}
	metricsByTableID := make(map[string]Metrics, 128)
	metricsByServerTableID := make(map[string]Metrics, 128)
	var metricsOfCluster []Metric
	metricsByAddr := make(map[string]Metrics, 128)
	for _, addr := range addrs {
		data, err := getOneServerMetrics(addr)
		if err != nil {
			log.Errorf("failed to get data from %s, err %s", addr, err)
			return
		}
		jsonData := gjson.Parse(data)
		for _, entity := range jsonData.Get("entities").Array() {
			etype := entity.Get("type").String()
			switch etype {
			case "replica":
			case "partition":
				tableID := entity.Get("attributes").Get("table_id").String()
				mergeIntoClusterLevelTableMetric(entity.Get("metrics").Array(),
					tableID, &metricsByTableID)
			case "table":
				tableID := entity.Get("attributes").Get("table_id").String()
				mergeIntoClusterLevelTableMetric(entity.Get("metrics").Array(),
					tableID, &metricsByTableID)
				collectServerLevelTableMetric(entity.Get("metrics").Array(), tableID,
					&metricsByServerTableID)
				collector.updateServerLevelTableMetrics(addr, metricsByServerTableID)
			case "server":
				mergeIntoClusterLevelServerMetric(entity.Get("metrics").Array(),
					metricsOfCluster)
				collectServerLevelServerMetrics(entity.Get("metrics").Array(),
					addr, &metricsByAddr)
			default:
				log.Errorf("Unsupport entity type %s", etype)
			}
		}
	}

	collector.updateClusterLevelTableMetrics(metricsByTableID)
	collector.updateServerLevelServerMetrics(metricsByAddr)
	collector.updateClusterLevelMetrics(metricsOfCluster)
}

// Update table metrics. They belong to a specified server.
func (collector *Collector) updateServerLevelTableMetrics(addr string, metricsByServerTableID map[string]Metrics) {
	for tableID, metrics := range metricsByServerTableID {
		var tableName string
		if name, ok := TableNameByID[tableID]; !ok {
			tableName = tableID
		} else {
			tableName = name
		}
		for _, metric := range metrics {
			collector.updateMetric(metric, addr, "server", tableName)
		}
	}
}

// Update server metrics. They belong to a specified server.
func (collector *Collector) updateServerLevelServerMetrics(metricsByAddr map[string]Metrics) {
	for addr, metrics := range metricsByAddr {
		for _, metric := range metrics {
			collector.updateMetric(metric, addr, "server", "server")
		}
	}
}

// Update cluster level metrics. They belong to a cluster.
func (collector *Collector) updateClusterLevelMetrics(metricsOfCluster []Metric) {
	for _, metric := range metricsOfCluster {
		collector.updateMetric(metric, "cluster", "server", metric.name)
	}
}

// Update table metrics. They belong to a cluster.
func (collector *Collector) updateClusterLevelTableMetrics(metricsByTableID map[string]Metrics) {
	for tableID, metrics := range metricsByTableID {
		var tableName string
		if name, ok := TableNameByID[tableID]; !ok {
			tableName = tableID
		} else {
			tableName = name
		}
		for _, metric := range metrics {
			collector.updateMetric(metric, "cluster", "table", tableName)
		}
	}
}

func (collector *Collector) updateMetric(metric Metric, endpoint string, level string, title string) {
	switch metric.mtype {
	case "Counter":
		if counter, ok := CounterMetricsMap[metric.name]; ok {
			counter.With(
				prometheus.Labels{"endpoint": endpoint,
					"role": collector.role, "level": level,
					"title": title}).Add(float64(metric.value))
		} else {
			log.Warnf("Unknown metric name %s", metric.name)
		}
	case "Gauge":
		if gauge, ok := GaugeMetricsMap[metric.name]; ok {
			gauge.With(
				prometheus.Labels{"endpoint": endpoint,
					"role": collector.role, "level": level,
					"title": title}).Set(float64(metric.value))
		} else {
			log.Warnf("Unknown metric name %s", metric.name)
		}
	case "Percentile":
		log.Warnf("Todo metric type %s", metric.mtype)
	case "Histogram":
	default:
		log.Warnf("Unsupport metric type %s", metric.mtype)
	}
}

func collectServerLevelTableMetric(metrics []gjson.Result, tableID string,
	metricsByServerTableID *map[string]Metrics) {
	var mts Metrics
	for _, metric := range metrics {
		name := metric.Get("name").String()
		mtype := metric.Get("type").String()
		value := metric.Get("value").Float()
		var values []float64
		if mtype == "percentile" {
			values = append(values, metric.Get("p50").Float())
			values = append(values, metric.Get("p90").Float())
			values = append(values, metric.Get("p95").Float())
			values = append(values, metric.Get("p99").Float())
			values = append(values, metric.Get("p999").Float())
		}
		m := Metric{name: name, mtype: mtype, value: value, values: values}
		mts = append(mts, m)
	}
	(*metricsByServerTableID)[tableID] = mts
}

func collectServerLevelServerMetrics(metrics []gjson.Result, addr string,
	metricsByAddr *map[string]Metrics) {
	var mts Metrics
	for _, metric := range metrics {
		name := metric.Get("name").String()
		mtype := metric.Get("type").String()
		value := metric.Get("value").Float()
		var values []float64
		if mtype == "percentile" {
			values = append(values, metric.Get("p50").Float())
			values = append(values, metric.Get("p90").Float())
			values = append(values, metric.Get("p95").Float())
			values = append(values, metric.Get("p99").Float())
			values = append(values, metric.Get("p999").Float())
		}
		m := Metric{name: name, mtype: mtype, value: value, values: values}
		mts = append(mts, m)
	}
	(*metricsByAddr)[addr] = mts
}

func mergeIntoClusterLevelServerMetric(metrics []gjson.Result, metricsOfCluster []Metric) {
	for _, metric := range metrics {
		name := metric.Get("name").String()
		mtype := metric.Get("type").String()
		value := metric.Get("value").Float()
		var isExisted bool = false
		for _, m := range metricsOfCluster {
			if m.name == name {
				isExisted = true
				switch mtype {
				case "Counter":
				case "Gauge":
					m.value += value
				case "Percentile":
					p50 := metric.Get("p50").Float()
					m.values[0] = math.Max(m.values[0], p50)
					p90 := metric.Get("p90").Float()
					m.values[1] = math.Max(m.values[0], p90)
					p95 := metric.Get("p95").Float()
					m.values[2] = math.Max(m.values[0], p95)
					p99 := metric.Get("p99").Float()
					m.values[3] = math.Max(m.values[0], p99)
					p999 := metric.Get("p999").Float()
					m.values[4] = math.Max(m.values[0], p999)
				case "Histogram":
				default:
					log.Errorf("Unsupport metric type %s", mtype)
				}
			}
		}
		if !isExisted {
			value := metric.Get("value").Float()
			var values []float64
			if mtype == "percentile" {
				values = append(values, metric.Get("p50").Float())
				values = append(values, metric.Get("p90").Float())
				values = append(values, metric.Get("p95").Float())
				values = append(values, metric.Get("p99").Float())
				values = append(values, metric.Get("p999").Float())
			}
			m := Metric{name: name, mtype: mtype, value: value, values: values}
			metricsOfCluster = append(metricsOfCluster, m)
		}
	}
}

func mergeIntoClusterLevelTableMetric(metrics []gjson.Result, tableID string,
	metricsByTableID *map[string]Metrics) {
	// Find a same table id, try to merge them.
	if _, ok := (*metricsByTableID)[tableID]; ok {
		mts := (*metricsByTableID)[tableID]
		for _, metric := range metrics {
			name := metric.Get("name").String()
			mtype := metric.Get("type").String()
			value := metric.Get("value").Float()
			for _, m := range mts {
				if name == m.name {
					switch mtype {
					case "Counter":
					case "Gauge":
						m.value += value
					case "Percentile":
						p50 := metric.Get("p50").Float()
						m.values[0] = math.Max(m.values[0], p50)
						p90 := metric.Get("p90").Float()
						m.values[1] = math.Max(m.values[0], p90)
						p95 := metric.Get("p95").Float()
						m.values[2] = math.Max(m.values[0], p95)
						p99 := metric.Get("p99").Float()
						m.values[3] = math.Max(m.values[0], p99)
						p999 := metric.Get("p999").Float()
						m.values[4] = math.Max(m.values[0], p999)
					case "Histogram":
					default:
						log.Errorf("Unsupport metric type %s", mtype)
					}
				}
			}
		}
	} else {
		var mts Metrics
		for _, metric := range metrics {
			name := metric.Get("name").String()
			mtype := metric.Get("type").String()
			value := metric.Get("value").Float()
			var values []float64
			if mtype == "percentile" {
				values = append(values, metric.Get("p50").Float())
				values = append(values, metric.Get("p90").Float())
				values = append(values, metric.Get("p95").Float())
				values = append(values, metric.Get("p99").Float())
				values = append(values, metric.Get("p999").Float())
			}
			m := Metric{name: name, mtype: mtype, value: value, values: values}
			mts = append(mts, m)
		}
		(*metricsByTableID)[tableID] = mts
	}
}

func getOneServerMetrics(addr string) (string, error) {
	url := fmt.Sprintf("http://%s/metrics?detail=true", addr)
	return httpGet(url)
}

func httpGet(url string) (string, error) {
	resp, err := http.Get(url)
	if err == nil && resp.StatusCode != http.StatusOK {
		err = errors.New(resp.Status)
	}
	if err != nil {
		log.Errorf("Fail to get data from %s, err %s", url, err)
		return "", err
	}
	body, _ := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	return string(body), nil
}

func getClusterInfo() (string, error) {
	addrs := viper.GetStringSlice("meta_servers")
	url := fmt.Sprintf("http://%s/meta/cluster", addrs[0])
	return httpGet(url)
}

func getTableInfo(pMetaServer string) (string, error) {
	url := fmt.Sprintf("http://%s/meta/apps", pMetaServer)
	return httpGet(url)
}

func updateClusterTableInfo() {
	// Get primary meta server address.
	data, err := getClusterInfo()
	if err != nil {
		log.Error("Fail to get cluster info")
		return
	}
	jsonData := gjson.Parse(data)
	pMetaServer := jsonData.Get("primary_meta_server").String()
	data, err = getTableInfo(pMetaServer)
	if err != nil {
		log.Error("Fail to get table info")
		return
	}
	jsonData = gjson.Parse(data)
	for _, value := range jsonData.Get("general_info").Map() {
		tableID := value.Get("app_id").String()
		tableName := value.Get("app_name").String()
		if _, ok := TableNameByID[tableID]; !ok {
			TableNameByID[tableID] = tableName
		}
	}
}
