package metrics

import "github.com/rcrowley/go-metrics"

// Metric
type Metric interface {
	Snapshot() float64

	Update(value float64)

	Name() string
}

func NewMetric(name string) Metric {
	return &metricImpl{
		name: name,
	}
}

type metricImpl struct {
	gauge metrics.GaugeFloat64
	name  string
}

func (m *metricImpl) Snapshot() float64 {
	return m.gauge.Snapshot().Value()
}

func (m *metricImpl) Update(v float64) {
	m.gauge.Update(v)
}

func (m *metricImpl) Name() string {
	return m.name
}
