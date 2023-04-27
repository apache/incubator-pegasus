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
	"sync/atomic"
	"time"

	"github.com/apache/incubator-pegasus/go-client/pegasus"
	log "github.com/sirupsen/logrus"
)

// Detector periodically checks the service availability of the Pegasus cluster.
type Detector interface {

	// Start detection until the ctx cancelled. This method will block the current thread.
	Start(ctx context.Context) error
}

// NewDetector returns a service-availability detector.
func NewDetector(client pegasus.Client) Detector {
	return &pegasusDetector{client: client}
}

type pegasusDetector struct {
	// client reads and writes periodically to a specified table.
	client      pegasus.Client
	detectTable pegasus.TableConnector

	detectInterval  time.Duration
	detectTableName string

	// timeout of a single detect
	detectTimeout time.Duration

	detectHashKeys [][]byte

	recentMinuteDetectTimes  uint64
	recentMinuteFailureTimes uint64

	recentHourDetectTimes  uint64
	recentHourFailureTimes uint64

	recentDayDetectTimes  uint64
	recentDayFailureTimes uint64
}

func (d *pegasusDetector) Start(rootCtx context.Context) error {
	var err error
	ctx, cancel := context.WithTimeout(rootCtx, 10*time.Second)
	defer cancel()
	d.detectTable, err = d.client.OpenTable(ctx, d.detectTableName)
	if err != nil {
		return err
	}

	ticker := time.NewTicker(d.detectInterval)
	for {
		select {
		case <-rootCtx.Done(): // check if context cancelled
			return nil
		case <-ticker.C:
			return nil
		default:
		}

		// periodically set/get a configured Pegasus table.
		d.detect(ctx)
	}
}

func (d *pegasusDetector) detect(rootCtx context.Context) {
	// TODO(yingchun): doesn't work, just to mute lint errors.
	d.detectPartition(rootCtx, 1)
}

func (d *pegasusDetector) detectPartition(rootCtx context.Context, partitionIdx int) {
	d.incrDetectTimes()

	go func() {
		ctx, cancel := context.WithTimeout(rootCtx, d.detectTimeout)
		defer cancel()

		hashkey := d.detectHashKeys[partitionIdx]
		value := []byte("")

		if err := d.detectTable.Set(ctx, hashkey, []byte(""), value); err != nil {
			d.incrFailureTimes()
			log.Errorf("set partition [%d] failed, hashkey=\"%s\": %s", partitionIdx, hashkey, err)
		}
		if _, err := d.detectTable.Get(ctx, hashkey, []byte("")); err != nil {
			d.incrFailureTimes()
			log.Errorf("get partition [%d] failed, hashkey=\"%s\": %s", partitionIdx, hashkey, err)
		}
	}()
}

func (d *pegasusDetector) incrDetectTimes() {
	atomic.AddUint64(&d.recentMinuteDetectTimes, 1)
	atomic.AddUint64(&d.recentHourDetectTimes, 1)
	atomic.AddUint64(&d.recentDayDetectTimes, 1)
}

func (d *pegasusDetector) incrFailureTimes() {
	atomic.AddUint64(&d.recentMinuteFailureTimes, 1)
	atomic.AddUint64(&d.recentHourFailureTimes, 1)
	atomic.AddUint64(&d.recentDayFailureTimes, 1)
}
