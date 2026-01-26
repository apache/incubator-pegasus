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
	MetaServers        []string
	RpcTimeout         time.Duration
	DetectInterval     time.Duration
	PullMetricsTimeout time.Duration
	SampleInterval     time.Duration
}

func LoadPartitionDetectorConfig() *PartitionDetectorConfig {
	return &PartitionDetectorConfig{
		MetaServers:        viper.GetStringSlice("meta_servers"),
		RpcTimeout:         viper.GetDuration("rpc_timeout"),
		DetectInterval:     viper.GetDuration("hotspot.partition_detect_interval"),
		PullMetricsTimeout: viper.GetDuration("hotspot.pull_metrics_timeout"),
		SampleInterval:     viper.GetDuration("hotspot.sample_interval"),
	}
}

var readMetricNames = []string{
	metrics.MetricReplicaGetRequests,
	metrics.MetricReplicaMultiGetRequests,
	metrics.MetricReplicaBatchGetRequests,
	metrics.MetricReplicaScanRequests,
}

var writeMetricNames = []string{
	metrics.MetricReplicaPutRequests,
	metrics.MetricReplicaMultiGetRequests,
	metrics.MetricReplicaRemoveRequests,
	metrics.MetricReplicaMultiRemoveRequests,
	metrics.MetricReplicaIncrRequests,
	metrics.MetricReplicaCheckAndSetRequests,
	metrics.MetricReplicaCheckAndMutateRequests,
	metrics.MetricReplicaDupRequests,
}

var metricFilter = metrics.NewMetricBriefValueFilter(
	[]string{metrics.MetricEntityTypeReplica},
	[]string{},
	map[string]string{},
	append(append([]string(nil), readMetricNames...), writeMetricNames...),
)

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

	if cfg.SampleInterval <= 0 {
		return nil, fmt.Errorf("SampleInterval(%d) must be > 0", cfg.SampleInterval)
	}

	if cfg.DetectInterval <= cfg.SampleInterval {
		return nil, fmt.Errorf("DetectInterval(%d) must be > SampleInterval(%d)",
			cfg.DetectInterval, cfg.SampleInterval)
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
	defer func(adminClient client.Client) {
		_ = adminClient.Close()
	}(adminClient)

	appMap, err := pullAppPartitions(adminClient)
	if err != nil {
		return err
	}

	err = d.aggregateMetrics(adminClient, appMap)
	if err != nil {
		return err
	}

	return nil
}

func pullAppPartitions(adminClient client.Client) (appStatsMap, error) {
	tables, err := adminClient.ListTables()
	if err != nil {
		return nil, err
	}

	appMap := make(appStatsMap)
	for _, table := range tables {
		appID, partitionCount, partitionConfigs, err := adminClient.QueryConfig(table.AppName)
		if err != nil {
			return nil, err
		}

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

func (d *partitionDetectorImpl) aggregateMetrics(adminClient client.Client, appMap appStatsMap) error {
	nodes, err := adminClient.ListNodes()
	if err != nil {
		return err
	}

	startSnapshots, err := d.pullMetrics(nodes)
	if err != nil {
		return err
	}

	time.Sleep(d.cfg.SampleInterval)

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
				stats[key] += operand
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

					stats[key] = (value - operand) / duration.Seconds()
				}
			}(time.Duration(endSnapshots[i].TimestampNS-snapshot.TimestampNS)),
			appMap)
	}

	return nil
}

func (d *partitionDetectorImpl) pullMetrics(nodes []*admin.NodeInfo) ([]*metrics.MetricQueryBriefValueSnapshot, error) {
	results := make([]*metrics.MetricQueryBriefValueSnapshot, len(nodes))

	ctx, cancel := context.WithTimeout(context.Background(), d.cfg.PullMetricsTimeout)
	defer cancel()

	eg, ctx := errgroup.WithContext(ctx)
	puller := func(index int, node *admin.NodeInfo) func() error {
		return func() error {
			metricClient := metrics.NewMetricClient(&metrics.MetricClientConfig{
				Host: node.HpNode.GetHost(),
				Port: node.HpNode.GetPort(),
			})

			snapshot, err := metricClient.GetBriefValueSnapshot(ctx, metricFilter)
			if err != nil {
				return err
			}

			results[index] = snapshot
			return nil
		}
	}

	for i, node := range nodes {
		eg.Go(puller(i, node))
	}

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
		if entity.Type != metrics.MetricEntityTypeReplica {
			continue
		}

		appID, err := strconv.Atoi(entity.Attributes[metrics.MetricEntityTableID])
		if err != nil {
			continue
		}

		stats, ok := appMap[int32(appID)]
		if !ok {
			continue
		}

		partitionID, err := strconv.Atoi(entity.Attributes[metrics.MetricEntityPartitionID])
		if err != nil {
			continue
		}

		if partitionID >= len(stats.partitionConfigs) {
			continue
		}

		primary := stats.partitionConfigs[partitionID].HpPrimary
		if primary.GetHost() != node.HpNode.GetHost() ||
			primary.GetPort() != node.HpNode.GetPort() {
			continue
		}

		for _, metric := range entity.Metrics {
			adder(stats.partitionStats[partitionID], metric.Name, metric.Value)
		}
	}
}
