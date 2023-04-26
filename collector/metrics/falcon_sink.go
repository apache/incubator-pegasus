package metrics

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/pegasus-kv/collector/aggregate"
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
	endpoint    string  // the cluster name
	metric      string  // metric name
	timestamp   int64   // the reporting time in unix seconds
	step        int32   // the reporting time interval in seconds
	value       float64 // metric value
	counterType string  // GAUGE or COUNTER
	tags        string
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
	m.metric = name
	m.value = value
	m.timestamp = time.Now().Unix()

	firstTag := true
	for k, v := range tags {
		if firstTag {
			firstTag = false
		} else {
			m.tags += ","
		}
		m.tags += k + "=" + v
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
		endpoint:    sink.cfg.clusterName,
		step:        int32(sink.cfg.metricsReportInterval.Seconds()),
		counterType: "GAUGE",
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
}
