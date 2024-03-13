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

package main

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"

	"github.com/apache/incubator-pegasus/collector/avail"
	"github.com/apache/incubator-pegasus/collector/hotspot"
	"github.com/apache/incubator-pegasus/collector/metrics"
	"github.com/apache/incubator-pegasus/collector/webui"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
	"gopkg.in/tomb.v2"
)

// callerPrettifier simplifies the caller info
func callerPrettifier(f *runtime.Frame) (function string, file string) {
	function = f.Function[strings.LastIndex(f.Function, "/")+1:]
	file = fmt.Sprint(f.File[strings.LastIndex(f.File, "/")+1:], ":", f.Line)
	return function, file
}

// setupSignalHandler setup signal handler for collector
func setupSignalHandler(shutdownFunc func()) {
	closeSignalChan := make(chan os.Signal, 1)
	signal.Notify(closeSignalChan,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-closeSignalChan
		log.Infof("got signal %s to exit", sig.String())
		shutdownFunc()
	}()
}

func main() {
	// initialize logging
	log.SetFormatter(&log.TextFormatter{
		DisableColors:    true,
		FullTimestamp:    true,
		CallerPrettyfier: callerPrettifier,
	})
	log.SetOutput(&lumberjack.Logger{ // rolling log
		Filename:  "./pegasus.log",
		MaxSize:   50, // MegaBytes
		MaxAge:    2,  // days
		LocalTime: true,
	})
	log.SetReportCaller(true)

	// TODO(wutao1): use args[1] as config path
	viper.SetConfigFile("config.yml")
	viper.SetConfigType("yaml")
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("failed to read config: ", err)
		return
	}

	registry := prometheus.NewRegistry()
	webui.StartWebServer(registry)

	tom := &tomb.Tomb{}
	setupSignalHandler(func() {
		tom.Kill(errors.New("Collector terminates")) // kill other goroutines
	})

	tom.Go(func() error {
		// Set detect inteverl and detect timeout 10s.
		return avail.NewDetector(10000000000, 10000000000, 16).Run(tom)
	})

	tom.Go(func() error {
		return metrics.NewMetaServerMetricCollector().Run(tom)
	})

	tom.Go(func() error {
		return metrics.NewReplicaServerMetricCollector().Run(tom)
	})

	tom.Go(func() error {
		conf := hotspot.PartitionDetectorConfig{
			DetectInterval: viper.GetDuration("hotspot.partition_detect_interval"),
		}
		return hotspot.NewPartitionDetector(conf).Run(tom)
	})

	err := tom.Wait()
	if err != nil {
		log.Error("Collector exited abnormally:", err)
		return
	}

	log.Info("Collector exited normally.")
}
