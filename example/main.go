// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/pegasus"
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
			sortKeys = append(sortKeys, []byte("sort"+string(i)))
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
