package metrics

import (
	"reflect"

	"github.com/rcrowley/go-metrics"
	log "github.com/sirupsen/logrus"
)

// Metric is a
type Metric interface {
	Update(value float64)

	Name() string
}

// NewMetric returns a Metric instance.
func NewMetric(name string) Metric {
	return &metricImpl{
		name:  name,
		gauge: metrics.GetOrRegisterGaugeFloat64(name, metrics.DefaultRegistry),
	}
}

type metricSnapshot struct {
	name  string
	value float64
	// TODO(wutao1): tags []string
}

// getAllSnapshots returns the instaneous snapshot of all metrics
func getAllSnapshots() (snapshots []*metricSnapshot) {
	metrics.DefaultRegistry.Each(func(name string, value interface{}) {
		m, ok := value.(metrics.GaugeFloat64)
		if !ok {
			log.Fatal("unexpected type of metrics: {}", reflect.TypeOf(value))
		}
		snapshots = append(snapshots, &metricSnapshot{
			name:  name,
			value: m.Snapshot().Value(),
		})
	})
	return
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
