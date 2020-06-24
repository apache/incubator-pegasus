package metrics

// Metric
type Metric interface {
	Snapshot() float64

	Merge(m Metric)

	Set(value float64)
}

func NewMetric(name string) Metric {

}
