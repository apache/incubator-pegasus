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
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/tomb.v2"
)

type HotspotDetector interface {
	Run(tom *tomb.Tomb) error
}

type Config struct {
	DetectInterval time.Duration
}

func NewHotspotDetector(conf Config) HotspotDetector {
	return &hotspotDetector{
		detectInterval: conf.DetectInterval,
	}
}

type hotspotDetector struct {
	detectInterval time.Duration
}

func (d *hotspotDetector) Run(tom *tomb.Tomb) error {
	ticker := time.NewTicker(d.detectInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.detect()
		case <-tom.Dying():
			log.Info("HotspotDetector exited.")
			return nil
		}
	}
}

func (d *hotspotDetector) detect() {
}
