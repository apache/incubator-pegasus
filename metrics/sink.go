package metrics

import (
	"log"

	"github.com/spf13/viper"
)

// Sink is the destination where the metrics are reported to.
type Sink interface {

	// Report the snapshot of metrics to the monitoring system. No retry even if it failed.
	Report()
}

type falconConfig struct {
	falconAgentHost     string `yaml:"falcon_agent.host"`
	falconAgentPort     uint16 `yaml:"falcon_agent.port"`
	falconAgentHTTPPath string `yaml:"falcon_agent.http_path"`
}

type falconSink struct {
	cfg *falconConfig
}

func (sink *falconSink) load() {
	sink.cfg = &falconConfig{}
	viper.Unmarshal(sink.cfg)
}

func (*falconSink) Report() {
}

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
