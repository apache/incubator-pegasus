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

func (sink *prometheusSink) load() {
	sink.cfg = &prometheusConfig{}
	viper.Unmarshal(sink.cfg)
}

func (sink *prometheusSink) Report() {

}

// NewSink creates a Sink which is a monitoring system.
func NewSink() Sink {
	cfgSink := viper.Get("metrics.sink")
	if cfgSink == "falcon" {
		sink := &falconSink{}
		sink.load()
		return sink
	} else if cfgSink == "prometheus" {
		sink := &prometheusSink{}
		sink.load()
		return sink
	} else {
		log.Fatalf("invalid metrics_sink = %s", cfgSink)
		return nil
	}
}
