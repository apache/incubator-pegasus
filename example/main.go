package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
)

func main() {
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

	time.Sleep(time.Second * 300)

	value := make([]byte, 1000)
	for i := 0; i < 1000; i++ {
		value[i] = 'x'
	}

	for t := 0; t < 10; t++ {
		for i := 0; i < 10; i++ {
			go func() {
				tb.Set(context.Background(), []byte("hash"), []byte("sort"), value)
			}()
			go func() {
				tb.Del(context.Background(), []byte("hash"), []byte("sort"))
			}()
		}

		time.Sleep(time.Second)
	}
}
