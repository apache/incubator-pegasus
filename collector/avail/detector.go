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

package avail

import (
	"context"
	"math/rand"
	"time"

	"github.com/apache/incubator-pegasus/go-client/admin"
	"github.com/apache/incubator-pegasus/go-client/pegasus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// Detector periodically checks the service availability of the Pegasus cluster.
type Detector interface {
	Run(tom *tomb.Tomb) error
}

// NewDetector returns a service-availability detector.
func NewDetector(detectInterval time.Duration,
	detectTimeout time.Duration, partitionCount int) Detector {
	metaServers := viper.GetStringSlice("meta_servers")
	tableName := viper.GetStringMapString("availablity_detect")["table_name"]
	// Create detect table.
	adminClient := admin.NewClient(admin.Config{MetaServers: metaServers})
	error := adminClient.CreateTable(context.Background(), tableName, partitionCount)
	if error != nil {
		log.Errorf("Create detect table %s failed, error: %s", tableName, error)
	}
	pegasusClient := pegasus.NewClient(pegasus.Config{MetaServers: metaServers})
	return &pegasusDetector{
		client:          pegasusClient,
		detectTableName: tableName,
		detectInterval:  detectInterval,
		detectTimeout:   detectTimeout,
		partitionCount:  partitionCount,
	}
}

var (
	DetectTimes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "detect_times",
		Help: "The times of availability detecting",
	})

	ReadFailureTimes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "read_failure_detect_times",
		Help: "The failure times of read detecting",
	})

	WriteFailureTimes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "write_failure_detect_times",
		Help: "The failure times of write detecting",
	})

	ReadLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "read_latency_ms",
		Help: "The latency of read data in milliseconds",
	})

	WriteLatency = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "write_latency_ms",
		Help: "The latency of write data in milliseconds",
	})
)

type pegasusDetector struct {
	client          pegasus.Client
	detectTable     pegasus.TableConnector
	detectTableName string
	detectInterval  time.Duration
	// timeout of a single detect.
	detectTimeout time.Duration
	// partition count.
	partitionCount int
}

func (d *pegasusDetector) Run(tom *tomb.Tomb) error {
	var err error
	// Open the detect table.
	d.detectTable, err = d.client.OpenTable(context.Background(), d.detectTableName)
	if err != nil {
		log.Errorf("Open detect table %s failed, error: %s", d.detectTable, err)
		return err
	}
	ticker := time.NewTicker(d.detectInterval)
	for {
		select {
		case <-tom.Dying():
			return nil
		case <-ticker.C:
			d.detectPartition()
		}
	}
}

func (d *pegasusDetector) detectPartition() {
	DetectTimes.Inc()

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), d.detectTimeout)
		defer cancel()
		value := []byte("test")
		// Select a partition randomly to be detected. That will ensure every partition
		// be detected.
		hashkey := []byte(RandStringBytes(d.partitionCount))
		now := time.Now().UnixMilli()
		if err := d.detectTable.Set(ctx, hashkey, []byte(""), value); err != nil {
			WriteFailureTimes.Inc()
			log.Errorf("Set hashkey \"%s\" failed, error: %s", hashkey, err)
		}
		ReadLatency.Set(float64(time.Now().UnixMilli() - now))
		now = time.Now().UnixMilli()
		if _, err := d.detectTable.Get(ctx, hashkey, []byte("")); err != nil {
			ReadFailureTimes.Inc()
			log.Errorf("Get hashkey \"%s\" failed, error: %s", hashkey, err)
		}
		WriteLatency.Set(float64(time.Now().UnixMilli() - now))
	}()
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// Generate a random string.
func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
