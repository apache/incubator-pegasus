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

package sink

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/apache/incubator-pegasus/collector/aggregate"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type falconConfig struct {
	falconAgentHost     string
	falconAgentPort     uint32
	falconAgentHTTPPath string

	clusterName           string
	port                  uint32
	metricsReportInterval time.Duration
}

type falconSink struct {
	cfg *falconConfig
}

type falconMetricData struct {
	Endpoint    string  // the cluster name
	Metric      string  // metric name
	Timestamp   int64   // the reporting time in unix seconds
	Step        int32   // the reporting time interval in seconds
	Value       float64 // metric value
	CounterType string  // GAUGE or COUNTER
	Tags        string
}

func newFalconSink() *falconSink {
	sink := &falconSink{}
	sink.cfg = &falconConfig{
		falconAgentHost:       viper.GetString("falcon_agent.host"),
		falconAgentPort:       viper.GetUint32("falcon_agent.port"),
		falconAgentHTTPPath:   viper.GetString("falcon_agent.http_path"),
		clusterName:           viper.GetString("cluster_name"),
		port:                  viper.GetUint32("port"),
		metricsReportInterval: viper.GetDuration("metrics.report_interval"),
	}
	return sink
}

func (m *falconMetricData) setData(name string, value float64, tags map[string]string) {
	m.Metric = name
	m.Value = value
	m.Timestamp = time.Now().Unix()

	firstTag := true
	for k, v := range tags {
		if firstTag {
			firstTag = false
		} else {
			m.Tags += ","
		}
		m.Tags += k + "=" + v
	}
}

func (m *falconMetricData) toJSON() []byte {
	result, err := json.Marshal(m)
	if err != nil {
		log.Fatal("failed to marshall falcon metric to json: ", err)
	}
	return result
}

type falconDataSerializer struct {
	buf *bytes.Buffer

	mdata *falconMetricData
}

func (s *falconDataSerializer) serialize(stats []aggregate.TableStats, allStats aggregate.ClusterStats) {
	s.buf.WriteString("[")
	for _, table := range stats {
		for name, value := range table.Stats {
			s.mdata.setData(name, value, map[string]string{
				"entity": "table",
				"table":  name,
			})
		}
		s.buf.Write(s.mdata.toJSON())
	}
	for name, value := range allStats.Stats {
		s.mdata.setData(name, value, map[string]string{
			"entity": "cluster",
		})
		s.buf.Write(s.mdata.toJSON())
	}
}

func (sink *falconSink) Report(stats []aggregate.TableStats, allStats aggregate.ClusterStats) {
	serializer := &falconDataSerializer{
		buf: bytes.NewBuffer(nil),
	}
	serializer.mdata = &falconMetricData{
		Endpoint:    sink.cfg.clusterName,
		Step:        int32(sink.cfg.metricsReportInterval.Seconds()),
		CounterType: "GAUGE",
	}
	serializer.serialize(stats, allStats)
	sink.postHTTPData(serializer.buf.Bytes())
}

func (sink *falconSink) postHTTPData(data []byte) {
	url := fmt.Sprintf("http://%s:%d/%s", sink.cfg.falconAgentHost, sink.cfg.falconAgentPort, sink.cfg.falconAgentHTTPPath)
	resp, err := http.Post(url, "application/x-www-form-urlencoded", bytes.NewReader([]byte(data)))
	if err == nil && resp.StatusCode != http.StatusOK {
		err = errors.New(resp.Status)
	}
	if err != nil {
		log.Errorf("failed to post metrics to falcon agent: %s", err)
		return
	}

	defer resp.Body.Close()
}
