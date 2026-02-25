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
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
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

func main() {
	loadConfigs()

	setupLog()

	registry := prometheus.NewRegistry()
	webui.StartWebServer(registry)

	// TODO(wangdan): consider replacing tomb since it has not been released since 2017.
	tom := &tomb.Tomb{}
	setupSignalHandler(func() {
		tom.Kill(nil) // kill other goroutines
	})

	tom.Go(func() error {
		// Set detect inteverl and detect timeout 10s.
		return avail.NewDetector(10000000000, 10000000000).Run(tom)
	})

	tom.Go(func() error {
		return metrics.NewMetaServerMetricCollector().Run(tom)
	})

	tom.Go(func() error {
		return metrics.NewReplicaServerMetricCollector().Run(tom)
	})

	tom.Go(func() error {
		partitionDetector, err := hotspot.NewPartitionDetector(hotspot.LoadPartitionDetectorConfig())
		if err != nil {
			log.Fatalf("failed to create partition detector for hotspot: %v", err)
		}

		return partitionDetector.Run(tom)
	})

	err := tom.Wait()
	if err != nil {
		log.Error("Collector exited abnormally: ", err)
		return
	}

	log.Info("Collector exited normally.")
}

func loadConfigs() {
	var configFile string
	flag.StringVar(&configFile, "config", "config.yml", "config file path")

	viper.SetConfigFile(configFile)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		fmt.Fprintf(os.Stderr, "failed to read config file in %s: %v\n", configFile, err)
		os.Exit(1)
	}
}

func setupLog() {
	options := viper.GetStringMapString("log")

	filename, ok := options["filename"]
	if !ok {
		// The log file is saved by default in /tmp.
		filename = filepath.Join(os.TempDir(), "collector.log")
	}
	if len(filename) == 0 {
		fmt.Fprintln(os.Stderr, "log.filename should not be empty")
		os.Exit(1)
	}

	maxFileSizeMBStr, ok := options["max_file_size_mb"]
	if !ok {
		// The max size of each log file is 64MB by default.
		maxFileSizeMBStr = "64"
	}

	maxFileSizeMB, err := strconv.Atoi(maxFileSizeMBStr)
	if err != nil || maxFileSizeMB <= 0 {
		fmt.Fprintf(os.Stderr, "log.max_file_size_mb(%s) is invalid: %v", maxFileSizeMBStr, err)
		os.Exit(1)
	}

	retentionDaysStr, ok := options["retention_days"]
	if !ok {
		// The log files are retained for 3 days by default.
		retentionDaysStr = "3"
	}

	retentionDays, err := strconv.Atoi(retentionDaysStr)
	if err != nil || retentionDays <= 0 {
		fmt.Fprintf(os.Stderr, "log.retention_period(%s) is invalid: %v", retentionDaysStr, err)
		os.Exit(1)
	}

	maxFileNumberStr, ok := options["max_file_number"]
	if !ok {
		// The max number of retained log files is 8 by default.
		maxFileNumberStr = "8"
	}

	maxFileNumber, err := strconv.Atoi(maxFileNumberStr)
	if err != nil || maxFileNumber <= 0 {
		fmt.Fprintf(os.Stderr, "log.max_file_number(%s) is invalid: %v", maxFileNumberStr, err)
		os.Exit(1)
	}

	levelStr, ok := options["level"]
	if !ok {
		levelStr = "info"
	}

	level, err := log.ParseLevel(levelStr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "log.level(%s) is invalid: %v", levelStr, err)
		os.Exit(1)
	}

	log.SetOutput(&lumberjack.Logger{ // rolling log
		Filename:   filename,
		MaxSize:    maxFileSizeMB,
		MaxAge:     retentionDays,
		MaxBackups: maxFileNumber,
		LocalTime:  true,
	})
	log.SetFormatter(&log.TextFormatter{
		DisableColors:    true,
		FullTimestamp:    true,
		CallerPrettyfier: callerPrettifier,
	})
	log.SetReportCaller(true)
	log.SetLevel(level)
}

// callerPrettifier simplifies the caller info.
func callerPrettifier(f *runtime.Frame) (function string, file string) {
	function = f.Function[strings.LastIndex(f.Function, "/")+1:]
	file = fmt.Sprint(f.File[strings.LastIndex(f.File, "/")+1:], ":", f.Line)
	return function, file
}

// setupSignalHandler setups signal handler for collector.
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
