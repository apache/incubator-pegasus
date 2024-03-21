/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/apache/incubator-pegasus/go-client/pegalog"
	"github.com/apache/incubator-pegasus/go-client/pegasus"
)

func main() {
	cfgPath, _ := filepath.Abs("./example/pegasus-client-config.json")
	rawCfg, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		fmt.Println(err)
		return
	}

	// customize where the pegasus-go-client's logs reside.
	pegalog.SetLogger(pegalog.NewLogrusLogger(&pegalog.LogrusConfig{
		MaxSize:    500, // megabytes
		MaxAge:     5,   // days
		MaxBackups: 100,
		Filename:   "./bin/pegasus.log",
	}))
	logger := pegalog.GetLogger()

	cfg := &pegasus.Config{}
	json.Unmarshal(rawCfg, cfg)
	c := pegasus.NewClient(*cfg)

	tb, err := c.OpenTable(context.Background(), "temp")
	if err != nil {
		return
	}

	value := make([]byte, 1000)
	for i := 0; i < 1000; i++ {
		value[i] = 'x'
	}

	for t := 0; t < 10; t++ {
		var sortKeys [][]byte
		for i := 0; i < 10; i++ {
			sortKeys = append(sortKeys, []byte("sort"+fmt.Sprint(i)))
		}
		for i := 0; i < 10; i++ {
			err = tb.Set(context.Background(), []byte("hash"), sortKeys[i], value)
			if err != nil {
				logger.Fatal(err)
			}
		}
		for i := 0; i < 10; i++ {
			_, err = tb.Get(context.Background(), []byte("hash"), sortKeys[i])
			if err != nil {
				logger.Fatal(err)
			}
		}
		_, _, err = tb.MultiGet(context.Background(), []byte("hash"), sortKeys)
		if err != nil {
			logger.Fatal(err)
		}
		time.Sleep(time.Second)
	}
}
