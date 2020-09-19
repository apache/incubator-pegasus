package metrics

type prometheusConfig struct {
	prometheusExposerPort uint16 `yaml:"prometheus_exposer.port"`
}

type prometheusSink struct {
	cfg *prometheusConfig
}

func (sink *prometheusSink) Report(snapshots []*MetricSnapshot) {

}
