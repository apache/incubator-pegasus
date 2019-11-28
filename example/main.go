package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	go func() {
		// http://localhost:2112/metrics
		http.Handle("/metrics", promhttp.Handler())
		http.ListenAndServe(":2112", nil)
	}()

	cfgPath, _ := filepath.Abs("./example/pegasus-client-config.json")
	rawCfg, err := ioutil.ReadFile(cfgPath)
	if err != nil {
		fmt.Println(err)
		return
	}

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
		for i := 0; i < 10; i++ {
			tb.Set(context.Background(), []byte("hash"), []byte("sort"), value)
		}
		for i := 0; i < 10; i++ {
			tb.Get(context.Background(), []byte("hash"), []byte("sort"))
		}

		time.Sleep(time.Second)
	}
}
