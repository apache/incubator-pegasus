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

package sink

import (
	"github.com/apache/incubator-pegasus/collector/aggregate"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
)

// Sink is the destination where the metrics are reported to.
type Sink interface {

	// Report the snapshot of metrics to the monitoring system. The report can possibly fail.
	Report(stats []aggregate.TableStats, allStats aggregate.ClusterStats)
}

// NewSink creates a Sink which reports metrics to the configured monitoring system.
func NewSink() Sink {
	var sink Sink
	cfgSink := viper.Get("metrics.sink")
	if cfgSink == "falcon" {
		sink = newFalconSink()
	} else if cfgSink == "prometheus" {
		sink = newPrometheusSink()
	} else {
		log.Fatalf("invalid metrics_sink = %s", cfgSink)
		return nil
	}

	aggregate.AddHookAfterTableStatEmitted(func(stats []aggregate.TableStats, allStats aggregate.ClusterStats) {
		go func() {
			sink.Report(stats, allStats)
		}()
	})

	return sink
}
