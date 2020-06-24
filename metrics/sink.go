package metrics

import (
	"log"

	"github.com/spf13/viper"
)

// Sink is the destination where the metrics are reported to.
type Sink interface {

	// Report the metrics to the monitoring system. No retry even if it failed.
	Report(metrics []Metric)
}

type falconConfig struct {
	falconAgentHost     string `"falcon_agent_host"`
	falconAgentPort     uint16 `"falcon_agent_port"`
	falconAgentHTTPPath string `"falcon_agent_http_path"`
}

type falconSink struct {
	cfg *falconConfig
}

func (*falconSink) load() {

}

func (*falconSink) Report(metrics []Metric) {

}

type prometheusConfig struct {
	exposerPort uint16 `"prometheus_exposer_port"`
}

type prometheusSink struct {
	cfg *prometheusConfig
}

func (*prometheusSink) load() {

}

func (*prometheusSink) Report(metrics []Metric) {

}

// NewSink creates a Sink which is a monitoring system.
func NewSink() Sink {
	cfgSink := viper.Get("metrics_sink")
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
