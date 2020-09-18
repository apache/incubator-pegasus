package metrics

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

type prometheusConfig struct {
	prometheusExposerPort uint16 `yaml:"prometheus_exposer.port"`
}

type prometheusSink struct {
	cfg *prometheusConfig
}

func (sink *prometheusSink) init() {
	sink.cfg = &prometheusConfig{}
	viper.Unmarshal(sink.cfg)
}

func (sink *prometheusSink) report(snapshots []*metricSnapshot) {

}

// newSink creates a Sink which is a monitoring system.
func newSink() metricSink {
	cfgSink := viper.Get("metrics.sink")
	if cfgSink == "falcon" {
		sink := &falconSink{}
		sink.init()
		return sink
	} else if cfgSink == "prometheus" {
		sink := &prometheusSink{}
		sink.init()
		return sink
	} else {
		log.Fatalf("invalid metrics_sink = %s", cfgSink)
		return nil
	}
}
