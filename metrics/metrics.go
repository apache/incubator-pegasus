package metrics

import "github.com/prometheus/client_golang/prometheus"

// Client metrics.
var (
	GetLatencyHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{Namespace: "pegasus",
			Subsystem: "client_go",
			Name:      "get_latency_ns",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 40),
		},
	)
	MultiGetLatencyHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{Namespace: "pegasus",
			Subsystem: "client_go",
			Name:      "multiget_latency_ns",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 40),
		},
	)
	SetLatencyHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{Namespace: "pegasus",
			Subsystem: "client_go",
			Name:      "set_latency_ns",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 40),
		},
	)
	MultiSetLatencyHistogram = prometheus.NewHistogram(
		prometheus.HistogramOpts{Namespace: "pegasus",
			Subsystem: "client_go",
			Name:      "multiset_latency_ns",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 40),
		},
	)
)

func init() {
	prometheus.MustRegister(GetLatencyHistogram)
	prometheus.MustRegister(MultiGetLatencyHistogram)
	prometheus.MustRegister(SetLatencyHistogram)
	prometheus.MustRegister(MultiSetLatencyHistogram)
}
