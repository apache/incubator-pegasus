// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package hotspot

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/apache/incubator-pegasus/collector/metrics"
	client "github.com/apache/incubator-pegasus/go-client/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"gopkg.in/tomb.v2"
)

type PartitionDetector interface {
	Run(tom *tomb.Tomb) error
}

type PartitionDetectorConfig struct {
	MetaServers           []string
	RpcTimeout            time.Duration
	DetectInterval        time.Duration
	PullMetricsTimeout    time.Duration
	SampleMetricsInterval time.Duration
}

func LoadPartitionDetectorConfig() *PartitionDetectorConfig {
	return &PartitionDetectorConfig{
		MetaServers:           viper.GetStringSlice("meta_servers"),
		RpcTimeout:            viper.GetDuration("hotspot.rpc_timeout"),
		DetectInterval:        viper.GetDuration("hotspot.partition_detect_interval"),
		PullMetricsTimeout:    viper.GetDuration("hotspot.pull_metrics_timeout"),
		SampleMetricsInterval: viper.GetDuration("hotspot.sample_metrics_interval"),
	}
}

func NewPartitionDetector(cfg *PartitionDetectorConfig) (PartitionDetector, error) {
	if len(cfg.MetaServers) == 0 {
		return nil, fmt.Errorf("MetaServers should not be empty")
	}

	if cfg.DetectInterval <= 0 {
		return nil, fmt.Errorf("DetectInterval(%d) must be > 0", cfg.DetectInterval)
	}

	if cfg.PullMetricsTimeout <= 0 {
		return nil, fmt.Errorf("PullMetricsTimeout(%d) must be > 0", cfg.PullMetricsTimeout)
	}

	if cfg.SampleMetricsInterval <= 0 {
		return nil, fmt.Errorf("SampleMetricsInterval(%d) must be > 0", cfg.SampleMetricsInterval)
	}

	if cfg.DetectInterval <= cfg.SampleMetricsInterval {
		return nil, fmt.Errorf("DetectInterval(%d) must be > SampleMetricsInterval(%d)",
			cfg.DetectInterval, cfg.SampleMetricsInterval)
	}

	return &partitionDetectorImpl{
		cfg: cfg,
	}, nil
}

type partitionDetectorImpl struct {
	cfg *PartitionDetectorConfig
}

func (d *partitionDetectorImpl) Run(tom *tomb.Tomb) error {
	ticker := time.NewTicker(d.cfg.DetectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.detect()
		case <-tom.Dying():
			log.Info("Hotspot partition detector exited.")
			return nil
		}
	}
}

func (d *partitionDetectorImpl) detect() {
	err := d.aggregate()
	if err != nil {
		log.Error("failed to aggregate metrics for hotspot: ", err)
	}
}

// {appID -> appStats}.
type appStatsMap map[int32]appStats

type appStats struct {
	appName          string
	partitionCount   int32
	partitionConfigs []*replication.PartitionConfiguration
	partitionStats   []map[string]float64 // {metric_name -> metric_value} for each partition.
}

func (d *partitionDetectorImpl) aggregate() error {
	adminClient := client.NewClient(client.Config{
		MetaServers: d.cfg.MetaServers,
		Timeout:     d.cfg.RpcTimeout,
	})
	defer adminClient.Close()

	// appMap is the final structure that includes all the statistical values.
	appMap, err := pullTablePartitions(adminClient)
	if err != nil {
		return err
	}

	err = d.aggregateMetrics(adminClient, appMap)
	if err != nil {
		return err
	}

	return nil
}

// Pull metadata of all available tables with all their partitions and form the final structure
// that includes all the statistical values.
func pullTablePartitions(adminClient client.Client) (appStatsMap, error) {
	tables, err := adminClient.ListTables()
	if err != nil {
		return nil, err
	}

	appMap := make(appStatsMap)
	for _, table := range tables {
		// Query metadata for each partition of each table.
		appID, partitionCount, partitionConfigs, err := adminClient.QueryConfig(table.AppName)
		if err != nil {
			return nil, err
		}

		// Initialize statistical value for each partition.
		partitionStats := make([]map[string]float64, 0, len(partitionConfigs))
		for range partitionConfigs {
			m := make(map[string]float64)
			partitionStats = append(partitionStats, m)
		}

		appMap[appID] = appStats{
			appName:          table.AppName,
			partitionCount:   partitionCount,
			partitionConfigs: partitionConfigs,
			partitionStats:   partitionStats,
		}
	}

	return appMap, nil
}

type aggregator func(map[string]float64, string, float64)

// Pull metrics from all nodes and aggregate them to produce the statistics.
func (d *partitionDetectorImpl) aggregateMetrics(adminClient client.Client, appMap appStatsMap) error {
	nodes, err := adminClient.ListNodes()
	if err != nil {
		return err
	}

	// Pull multiple results of metrics to perform cumulative calculation to produce the
	// statistics such as QPS.
	startSnapshots, err := d.pullMetrics(nodes)
	if err != nil {
		return err
	}

	time.Sleep(d.cfg.SampleMetricsInterval)

	endSnapshots, err := d.pullMetrics(nodes)
	if err != nil {
		return err
	}

	for i, snapshot := range endSnapshots {
		if snapshot.TimestampNS <= startSnapshots[i].TimestampNS {
			return fmt.Errorf("end timestamp (%d) must be greater than start timestamp (%d)",
				snapshot.TimestampNS, startSnapshots[i].TimestampNS)
		}

		d.calculateStats(snapshot, nodes[i],
			func(stats map[string]float64, key string, operand float64) {
				// Just set the ending number of requests.
				stats[key] = operand
			},
			appMap)
	}

	for i, snapshot := range startSnapshots {
		d.calculateStats(snapshot, nodes[i],
			func(duration time.Duration) aggregator {
				return func(stats map[string]float64, key string, operand float64) {
					value, ok := stats[key]
					if !ok || value < operand {
						stats[key] = 0
						return
					}

					// Calculate QPS based on the ending number of requests that have been
					// set previously.
					stats[key] = (value - operand) / duration.Seconds()
				}
			}(time.Duration(endSnapshots[i].TimestampNS-snapshot.TimestampNS)),
			appMap)
	}

	return nil
}

var (
	readMetricNames = []string{
		metrics.MetricReplicaGetRequests,
		metrics.MetricReplicaMultiGetRequests,
		metrics.MetricReplicaBatchGetRequests,
		metrics.MetricReplicaScanRequests,
	}

	writeMetricNames = []string{
		metrics.MetricReplicaPutRequests,
		metrics.MetricReplicaMultiGetRequests,
		metrics.MetricReplicaRemoveRequests,
		metrics.MetricReplicaMultiRemoveRequests,
		metrics.MetricReplicaIncrRequests,
		metrics.MetricReplicaCheckAndSetRequests,
		metrics.MetricReplicaCheckAndMutateRequests,
		metrics.MetricReplicaDupRequests,
	}

	metricFilter = metrics.NewMetricBriefValueFilter(
		[]string{metrics.MetricEntityTypeReplica},
		[]string{},
		map[string]string{},
		append(append([]string(nil), readMetricNames...), writeMetricNames...),
	)
)

func (d *partitionDetectorImpl) pullMetrics(nodes []*admin.NodeInfo) ([]*metrics.MetricQueryBriefValueSnapshot, error) {
	results := make([]*metrics.MetricQueryBriefValueSnapshot, len(nodes))

	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.PullMetricsTimeout)
	defer cancel()

	// Pull the metrics simultaneously from all nodes.
	eg, ctx := errgroup.WithContext(ctx)
	puller := func(index int, node *admin.NodeInfo) func() error {
		return func() error {
			// Create a client for each target node.
			metricClient := metrics.NewMetricClient(&metrics.MetricClientConfig{
				Host: node.HpNode.GetHost(),
				Port: node.HpNode.GetPort(),
			})

			// Pull the metrics from the target node.
			snapshot, err := metricClient.GetBriefValueSnapshot(ctx, metricFilter)
			if err != nil {
				return err
			}

			// Place the pulled result into the position in the slice that correspond to
			// the target node.
			results[index] = snapshot
			return nil
		}
	}

	for i, node := range nodes {
		// Launch one Go routine for each target node to pull metrics from it.
		eg.Go(puller(i, node))
	}

	// Wait all requests to be finished.
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return results, nil
}

func (d *partitionDetectorImpl) calculateStats(
	snapshot *metrics.MetricQueryBriefValueSnapshot,
	node *admin.NodeInfo,
	adder aggregator,
	appMap appStatsMap) {
	for _, entity := range snapshot.Entities {
		// The metric must belong to the entity of "replica".
		if entity.Type != metrics.MetricEntityTypeReplica {
			continue
		}

		// The metric must have valid table id.
		appID, err := strconv.Atoi(entity.Attributes[metrics.MetricEntityTableID])
		if err != nil {
			continue
		}

		// The table must exist in the returned metadata, which means it is available.
		stats, ok := appMap[int32(appID)]
		if !ok {
			continue
		}

		// The metric must have valid partition id.
		partitionID, err := strconv.Atoi(entity.Attributes[metrics.MetricEntityPartitionID])
		if err != nil {
			continue
		}

		// The partition id should be less than the number of partitions.
		if partitionID >= len(stats.partitionConfigs) {
			continue
		}

		// Only primary replica of a partition will be counted.
		// TODO(wangdan): support Equal() for base.HostPort.
		primary := stats.partitionConfigs[partitionID].HpPrimary
		if primary.GetHost() != node.HpNode.GetHost() ||
			primary.GetPort() != node.HpNode.GetPort() {
			continue
		}

		for _, metric := range entity.Metrics {
			// Perform cumulative calculation for each statistical value.
			adder(stats.partitionStats[partitionID], metric.Name, metric.Value)
		}
	}
}
