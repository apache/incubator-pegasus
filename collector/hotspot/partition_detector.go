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
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/apache/incubator-pegasus/collector/metrics"
	client "github.com/apache/incubator-pegasus/go-client/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/admin"
	"github.com/apache/incubator-pegasus/go-client/idl/replication"
	"github.com/gammazero/deque"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/sync/errgroup"
	"gopkg.in/tomb.v2"
)

type PartitionDetector interface {
	Run(tom *tomb.Tomb) error
}

type PartitionDetectorConfig struct {
	MetaServers              []string
	RetentionPeriod          time.Duration
	RpcTimeout               time.Duration
	DetectInterval           time.Duration
	PullMetricsTimeout       time.Duration
	SampleMetricsInterval    time.Duration
	MaxSampleSize            int
	HotspotPartitionMinScore float64
	HotspotPartitionMinQPS   float64
}

func LoadPartitionDetectorConfig() *PartitionDetectorConfig {
	return &PartitionDetectorConfig{
		MetaServers:              viper.GetStringSlice("meta_servers"),
		RetentionPeriod:          viper.GetDuration("hotspot.retention_period"),
		RpcTimeout:               viper.GetDuration("hotspot.rpc_timeout"),
		DetectInterval:           viper.GetDuration("hotspot.partition_detect_interval"),
		PullMetricsTimeout:       viper.GetDuration("hotspot.pull_metrics_timeout"),
		SampleMetricsInterval:    viper.GetDuration("hotspot.sample_metrics_interval"),
		MaxSampleSize:            viper.GetInt("hotspot.max_sample_size"),
		HotspotPartitionMinScore: viper.GetFloat64("hotspot.hotspot_partition_min_score"),
		HotspotPartitionMinQPS:   viper.GetFloat64("hotspot.hotspot_partition_min_qps"),
	}
}

func NewPartitionDetector(cfg *PartitionDetectorConfig) (PartitionDetector, error) {
	if len(cfg.MetaServers) == 0 {
		return nil, fmt.Errorf("MetaServers should not be empty")
	}

	if cfg.RetentionPeriod <= 0 {
		return nil, fmt.Errorf("RetentionPeriod(%d) must be > 0", cfg.RetentionPeriod)
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

	if cfg.MaxSampleSize <= 0 {
		return nil, fmt.Errorf("MaxSampleSize(%d) must be > 0", cfg.MaxSampleSize)
	}

	if cfg.HotspotPartitionMinScore <= 0 {
		return nil, fmt.Errorf("HotspotPartitionMinScore(%f) must be > 0", cfg.HotspotPartitionMinScore)
	}

	if cfg.HotspotPartitionMinQPS <= 0 {
		return nil, fmt.Errorf("HotspotPartitionMinQPS (%f) must be > 0", cfg.HotspotPartitionMinQPS)
	}

	return &partitionDetectorImpl{
		cfg:       cfg,
		analyzers: make(map[partitionAnalyzerKey]*partitionAnalyzer),
	}, nil
}

type partitionDetectorImpl struct {
	cfg       *PartitionDetectorConfig
	mtx       sync.RWMutex
	analyzers map[partitionAnalyzerKey]*partitionAnalyzer
}

func (d *partitionDetectorImpl) Run(tom *tomb.Tomb) error {
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go d.checkExpiration(ctx, &wg)

	ticker := time.NewTicker(d.cfg.DetectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.detect()
		case <-tom.Dying():
			cancel()
			wg.Wait()

			log.Info("Hotspot partition detector exited.")
			return nil
		}
	}
}

func (d *partitionDetectorImpl) checkExpiration(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(d.cfg.RetentionPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.retireExpiredTables()

		case <-ctx.Done():
			log.Info("Expiration checker for hotspot exited.")
			return
		}
	}
}

func (d *partitionDetectorImpl) retireExpiredTables() {
	currentTimestampSeconds := time.Now().Unix()

	d.mtx.Lock()
	defer d.mtx.Unlock()

	log.Info("check expired tables")

	for key, analyzer := range d.analyzers {
		if !analyzer.isExpired(currentTimestampSeconds) {
			continue
		}

		delete(d.analyzers, key)
	}
}

func (d *partitionDetectorImpl) detect() {
	appMap, err := d.aggregate()
	if err != nil {
		log.Error("failed to aggregate metrics for hotspot: ", err)
	}

	log.Debugf("stats=%v", appMap)

	d.analyse(appMap)
}

// {appID -> appStats}.
type appStatsMap map[int32]appStats

type appStats struct {
	appName          string
	partitionCount   int32
	partitionConfigs []*replication.PartitionConfiguration
	partitionStats   []map[string]float64 // {metricName -> metricValue} for each partition.
}

func (d *partitionDetectorImpl) aggregate() (appStatsMap, error) {
	// appMap includes the structures that hold all the final statistical values.
	appMap, nodes, err := d.fetchMetadata()
	if err != nil {
		return nil, err
	}

	err = d.aggregateMetrics(appMap, nodes)
	if err != nil {
		return nil, err
	}

	return appMap, nil
}

// Fetch necessary metadata from meta server for the aggregation of metrics, including:
// - the metadata of all available tables with all their partitions, and
// - the node information of all replica servers.
// Also, the returned appStatsMap includes the structures that hold all the final
// statistical values.
func (d *partitionDetectorImpl) fetchMetadata() (appStatsMap, []*admin.NodeInfo, error) {
	adminClient := client.NewClient(client.Config{
		MetaServers: d.cfg.MetaServers,
		Timeout:     d.cfg.RpcTimeout,
	})
	defer adminClient.Close()

	// Fetch the information of all available tables.
	tables, err := adminClient.ListTables()
	if err != nil {
		return nil, nil, err
	}

	appMap := make(appStatsMap)
	for _, table := range tables {
		// Query metadata for each partition of each table.
		appID, partitionCount, partitionConfigs, err := adminClient.QueryConfig(table.AppName)
		if err != nil {
			return nil, nil, err
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

	// Fetch the node information of all replica servers.
	nodes, err := adminClient.ListNodes()
	if err != nil {
		return nil, nil, err
	}

	return appMap, nodes, nil
}

type aggregator func(map[string]float64, string, float64)

// Pull metric samples from nodes and aggregate them to produce the final statistical results
// into appMap.
func (d *partitionDetectorImpl) aggregateMetrics(appMap appStatsMap, nodes []*admin.NodeInfo) error {
	// Pull multiple samples of metrics to perform cumulative calculation to produce the
	// statistical results such as QPS.
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

		calculateStats(snapshot, nodes[i],
			func(stats map[string]float64, key string, operand float64) {
				// Just set the number of requests with ending snapshot.
				stats[key] = operand
			},
			appMap)
	}

	for i, snapshot := range startSnapshots {
		calculateStats(snapshot, nodes[i],
			func(duration time.Duration) aggregator {
				return func(stats map[string]float64, key string, operand float64) {
					value, ok := stats[key]
					if !ok || value < operand {
						stats[key] = 0
						return
					}

					// Calculate QPS based on ending snapshot that have been
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

func calculateStats(
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
		primary := stats.partitionConfigs[partitionID].HpPrimary
		if !node.HpNode.Equal(primary) {
			continue
		}

		for _, metric := range entity.Metrics {
			// Perform cumulative calculation for each statistical value.
			adder(stats.partitionStats[partitionID], metric.Name, metric.Value)
		}
	}
}

// Since the partition number of a table might be changed, use (appID, partitionCount)
// pair as the key for each table.
type partitionAnalyzerKey struct {
	appID          int32
	partitionCount int32
}

const (
	readHotspotData = iota
	writeHotspotData
	operateHotspotDataNumber
)

// hotspotPartitionStats holds all the statistical values of each partition, used for analysis
// of hotspot partitions.
type hotspotPartitionStats struct {
	totalQPS [operateHotspotDataNumber]float64
}

// Receive statistical values of all kinds of reads and writes, and by aggregating them we
// can obtain the overall statistics of reads and writes.
func calculateHotspotStats(appMap appStatsMap) map[partitionAnalyzerKey][]hotspotPartitionStats {
	results := make(map[partitionAnalyzerKey][]hotspotPartitionStats)
	for appID, stats := range appMap {
		partitionCount := len(stats.partitionStats)
		value := make([]hotspotPartitionStats, 0, partitionCount)

		for _, partitionStats := range stats.partitionStats {
			var hotspotStats hotspotPartitionStats

			// Calculate total QPS over all kinds of reads.
			for _, metricName := range readMetricNames {
				hotspotStats.totalQPS[readHotspotData] += partitionStats[metricName]
			}

			// Calculate total QPS over all kinds of writes.
			for _, metricName := range writeMetricNames {
				hotspotStats.totalQPS[writeHotspotData] += partitionStats[metricName]
			}

			value = append(value, hotspotStats)
		}

		key := partitionAnalyzerKey{appID: appID, partitionCount: int32(partitionCount)}
		results[key] = value
	}

	return results
}

// Calculate statistical values over multiples tables with all partitions of each table as
// a sample, and analyse all samples of each table asynchronously to decide which partitions
// of it are hotspots.
func (d *partitionDetectorImpl) analyse(appMap appStatsMap) {
	hotspotMap := calculateHotspotStats(appMap)

	nowTime := time.Now()
	expireTime := nowTime.Add(d.cfg.RetentionPeriod)
	expireTimestampSeconds := expireTime.Unix()

	d.mtx.Lock()
	defer d.mtx.Unlock()

	for key, value := range hotspotMap {
		analyzer, ok := d.analyzers[key]
		if !ok {
			analyzer = newPartitionAnalyzer(
				d.cfg.MaxSampleSize,
				d.cfg.HotspotPartitionMinScore,
				d.cfg.HotspotPartitionMinQPS,
				key.appID,
				key.partitionCount,
			)
			d.analyzers[key] = analyzer
		}

		analyzer.add(value, expireTimestampSeconds)

		// Perform the analysis asynchronously.
		go analyzer.analyse()
	}
}

func newPartitionAnalyzer(
	maxSampleSize int,
	hotspotPartitionMinScore float64,
	hotspotPartitionMinQPS float64,
	appID int32,
	partitionCount int32,
) *partitionAnalyzer {
	return &partitionAnalyzer{
		maxSampleSize:            maxSampleSize,
		hotspotPartitionMinScore: hotspotPartitionMinScore,
		hotspotPartitionMinQPS:   hotspotPartitionMinQPS,
		appID:                    appID,
		partitionCount:           partitionCount,
	}
}

// partitionAnalyzer holds the samples for all partitions of a table and analyses hotspot
// partitions based on them.
type partitionAnalyzer struct {
	// TODO(wangdan): bump gammazero/deque to the lastest version after upgrading Go to 1.23+,
	// since older Go versions do not support the `Deque.Iter()` iterator interface.
	maxSampleSize            int
	hotspotPartitionMinScore float64
	hotspotPartitionMinQPS   float64
	appID                    int32
	partitionCount           int32
	mtx                      sync.RWMutex
	expireTimestampSeconds   int64
	samples                  deque.Deque[[]hotspotPartitionStats] // Each element is a sample of all partitions of the table
}

func (a *partitionAnalyzer) isExpired(currentTimestampSeconds int64) bool {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	return currentTimestampSeconds >= a.expireTimestampSeconds
}

func (a *partitionAnalyzer) add(
	sample []hotspotPartitionStats,
	expireTimestampSeconds int64,
) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	a.expireTimestampSeconds = expireTimestampSeconds

	for a.samples.Len() >= a.maxSampleSize {
		a.samples.PopFront()
	}

	a.samples.PushBack(sample)
	log.Debugf("appID=%d, partitionCount=%d, samples=%v", a.appID, a.partitionCount, a.samples)
}

func (a *partitionAnalyzer) analyse() {
	a.mtx.RLock()
	defer a.mtx.RUnlock()

	a.analyseHotspots(readHotspotData)
	a.analyseHotspots(writeHotspotData)
}

func (a *partitionAnalyzer) analyseHotspots(operationType int) {
	sample, scores := a.calculateScores(operationType)
	if len(scores) == 0 {
		return
	}

	hotspotCount := a.countHotspots(operationType, sample, scores)

	// TODO(wangdan): export the hotspot-related metrics for collection by monitoring
	// systems such as Prometheus.
	log.Infof("appID=%d, partitionCount=%d, operationType=%d, hotspotPartitions=%d, scores=%v",
		a.appID, a.partitionCount, operationType, hotspotCount, scores)
}

// Calculates [Z-score](https://en.wikipedia.org/wiki/Standard_score) for each partition by
// comparing historical data vertically and concurrent data horizontally to describe the
// hotspots.
func (a *partitionAnalyzer) calculateScores(
	operationType int,
) (
	[]hotspotPartitionStats,
	[]float64,
) {
	var count int
	var partitionQPSSum float64
	// TODO(wangdan): use `range a.samples.Iter()` instead for Go 1.23+.
	for i, n := 0, a.samples.Len(); i < n; i++ {
		sample := a.samples.At(i)
		count += len(sample)
		for _, stats := range sample {
			partitionQPSSum += stats.totalQPS[operationType]
		}
	}

	if count <= 1 {
		log.Infof("sample size(%d) <= 1, not enough data for calculation", count)
		return nil, nil
	}

	partitionQPSAvg := partitionQPSSum / float64(count)

	var standardDeviation float64
	// TODO(wangdan): use `range a.samples.Iter()` instead for Go 1.23+.
	for i, n := 0, a.samples.Len(); i < n; i++ {
		for _, stats := range a.samples.At(i) {
			deviation := stats.totalQPS[operationType] - partitionQPSAvg
			standardDeviation += deviation * deviation
		}
	}

	standardDeviation = math.Sqrt(standardDeviation / float64(count-1))

	sample := a.samples.Back()
	scores := make([]float64, 0, len(sample))
	for i := 0; i < len(sample); i++ {
		if standardDeviation == 0 {
			scores = append(scores, 0)
			continue
		}

		score := (sample[i].totalQPS[operationType] - partitionQPSAvg) / standardDeviation
		scores = append(scores, score)
	}

	return sample, scores
}

func (a *partitionAnalyzer) countHotspots(
	operationType int,
	sample []hotspotPartitionStats,
	scores []float64,
) (hotspotCount int) {
	for i, score := range scores {
		if score < a.hotspotPartitionMinScore {
			continue
		}

		if sample[i].totalQPS[operationType] < a.hotspotPartitionMinQPS {
			continue
		}

		hotspotCount++
	}

	return
}
