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

package usage

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/incubator-pegasus/collector/aggregate"
	"github.com/apache/incubator-pegasus/go-client/pegasus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/tomb.v2"
)

// TableUsageRecorder records the usage of each table into a Pegasus table.
// The usage statistics can be used for service cost calculation.
type TableUsageRecorder interface {

	// Start recording until the ctx cancelled. This method will block the current thread.
	Start(tom *tomb.Tomb)
}

// NewTableUsageRecorder returns an instance of TableUsageRecorder
func NewTableUsageRecorder() TableUsageRecorder {
	return &tableUsageRecorder{
		usageStatApp: viper.GetString("usage_stat_app"),
	}
}

type tableUsageRecorder struct {
	client pegasus.Client
	table  pegasus.TableConnector

	usageStatApp string
}

func (rec *tableUsageRecorder) Start(tom *tomb.Tomb) {
	if rec.usageStatApp == "" {
		// if no stat app is specified, usage recorder is considered as disabled.
		return
	}

	metaServer := viper.GetString("meta_server")
	rec.client = pegasus.NewClient(pegasus.Config{MetaServers: []string{metaServer}})
	for {
		var err error
		rec.table, err = rec.client.OpenTable(tom.Context(context.TODO()), rec.usageStatApp)
		if err != nil {
			// retry indefinitely
			log.Errorf("failed to open table: %s", err.Error())
			sleepWait(tom, 15*time.Second)
			continue
		}
		break
	}

	aggregate.AddHookAfterTableStatEmitted(func(stats []aggregate.TableStats, allStat aggregate.ClusterStats) {
		rootCtx := tom.Context(context.TODO())
		for _, s := range stats {
			rec.writeTableUsage(rootCtx, &s)
		}
	})
}

func sleepWait(tom *tomb.Tomb, waitTime time.Duration) {
	ticker := time.NewTicker(waitTime)
	select {
	case <-tom.Dying():
		return
	case <-ticker.C:
	}
}

func (rec *tableUsageRecorder) writeTableUsage(ctx context.Context, tb *aggregate.TableStats) {
	hashKey := []byte(fmt.Sprintf("%d", tb.Timestamp.Unix()))
	sortkey := []byte("cu")

	readCU := tb.Stats["recent_read_cu"]
	writeCU := tb.Stats["recent_write_cu"]
	value := []byte(fmt.Sprintf("{\"%d\":[%f, %f]}", tb.AppID, readCU, writeCU))

	go func() {
		maxRetryCount := 10
		for maxRetryCount > 0 {
			// TODO(wutao): set rpc timeout
			err := rec.table.Set(ctx, hashKey, sortkey, value)
			if err == nil {
				break
			}
			log.Errorf("failed to write cu [timestamp: %s, appid: %d, readcu: %f, writecu: %f]",
				tb.Timestamp.Local().String(),
				tb.AppID,
				readCU,
				writeCU)
			maxRetryCount--
		}
	}()
}
