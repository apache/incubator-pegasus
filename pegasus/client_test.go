// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestPegasusClient_OpenTable(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	tb1, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	assert.NotNil(t, tb1)

	tb2, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	assert.NotNil(t, tb1)

	// must reuse previous connection
	assert.Equal(t, tb1, tb2)

	pclient, _ := client.(*pegasusClient)
	assert.NotNil(t, pclient.findTable("temp"))

	tb, err := client.OpenTable(context.Background(), "table_not_exists")
	assert.NotNil(t, err)
	assert.Nil(t, tb)
}

func TestPegasusClient_OpenTableTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	// make sure the port 8801 is not opened on your computer.
	cfg := Config{
		MetaServers: []string{"0.0.0.0:8801"},
	}

	client := NewClient(cfg)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	tb, err := client.OpenTable(ctx, "temp")
	assert.Equal(t, ctx.Err(), context.DeadlineExceeded)
	assert.Nil(t, tb)
	assert.NotNil(t, err)

	client.Close()
}

// Ensure that concurrent OpenTable operations to the same table
// won't invoke more than one query to meta server.
func TestPegasusClient_ConcurrentOpenSameTable(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	openTableQueries := 100
	tblist := make([]TableConnector, openTableQueries)

	var wg sync.WaitGroup
	wg.Add(openTableQueries)
	for i := 0; i < openTableQueries; i++ {
		idx := i
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			tb, err := client.OpenTable(ctx, "temp")
			assert.Nil(t, err)
			tblist[idx] = tb
			wg.Done()
		}()
	}
	wg.Wait()

	// all tables returned by OpenTable must be the same one
	tb := tblist[0]
	for i := 1; i < openTableQueries; i++ {
		assert.Equal(t, tb, tblist[i])
	}
}

// In this test we verifies if there's any easy bugs can be found in concurrent rpc.
func TestPegasusClient_ConcurrentMetaQueries(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)

		id := i
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, err := client.OpenTable(ctx, "table_not_exist"+fmt.Sprint(id))
			assert.NotNil(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_New(t *testing.T) {
	c, err := newClientWithError(Config{
		MetaServers: []string{"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"},
	})
	assert.Nil(t, err)
	_ = c.Close()

	c, err = newClientWithError(Config{
		MetaServers: []string{"127abc"},
	})
	assert.NotNil(t, err)
	assert.Nil(t, c)

	_, err = newClientWithError(Config{
		MetaServers: []string{},
	})
	assert.NotNil(t, err)
}

func compareMaps(t *testing.T, dataMap map[string]string, baseMap map[string]string) error {
	//sort dataMap & sort baseMap & compare
	assert.Equal(t, len(baseMap), len(dataMap))
	dataKey := make([]string, len(dataMap))
	baseKey := make([]string, len(baseMap))
	i := 0
	for k := range dataMap {
		dataKey[i] = k
		i++
	}
	i = 0
	for k := range baseMap {
		baseKey[i] = k
		i++
	}

	sort.Strings(dataKey)
	sort.Strings(baseKey)

	for i := 0; i < len(dataKey); i++ {
		assert.Equal(t, baseKey[i], dataKey[i])
		assert.Equal(t, baseMap[baseKey[i]], dataMap[dataKey[i]])
	}

	return nil
}

func cutAndCompareMaps(t *testing.T, dataMap map[string]string, baseMap map[string]string,
	start []byte, startInclusive bool, stop []byte, stopInclusive bool) error {
	if len(dataMap) == 0 {
		return fmt.Errorf("can't cut the baseMap, abandon compare")
	}
	//sort dataMap & sort baseMap & cut baseMap & compare
	dataKey := make([]string, len(dataMap))
	baseKey := make([]string, len(baseMap))
	i := 0
	for k := range dataMap {
		dataKey[i] = k
		i++
	}
	i = 0
	for k := range baseMap {
		baseKey[i] = k
		i++
	}
	sort.Strings(dataKey)
	sort.Strings(baseKey)

	begin := sort.SearchStrings(baseKey, string(start))
	if !startInclusive {
		begin++
	}
	end := sort.SearchStrings(baseKey, string(stop))
	if !stopInclusive {
		end--
	}

	baseKeySlice := baseKey[begin : end+1]
	assert.Equal(t, len(baseKeySlice), len(dataKey))
	for i := 0; i < len(dataKey); i++ {
		assert.Equal(t, baseKeySlice[i], dataKey[i])
		assert.Equal(t, baseMap[baseKeySlice[i]], dataMap[dataKey[i]])
	}
	return nil
}

func compareAll(t *testing.T, dataMap map[string]string, baseMap map[string]map[string]string) error {
	//merge baseMap & sort both & compare
	baseMapAll := make(map[string]string)
	for mapName, mapItem := range baseMap {
		for k, v := range mapItem {
			blob := encodeHashKeySortKey([]byte(mapName), []byte(k))
			baseMapAll[string(blob.Data)] = string(v)
		}
	}

	assert.Equal(t, len(baseMapAll), len(dataMap))
	dataKey := make([]string, len(dataMap))
	baseKey := make([]string, len(baseMapAll))
	i := 0
	for k := range dataMap {
		dataKey[i] = k
		i++
	}
	i = 0
	for k := range baseMapAll {
		baseKey[i] = k
		i++
	}

	sort.Strings(dataKey)
	sort.Strings(baseKey)

	for i := 0; i < len(dataKey); i++ {
		assert.Equal(t, baseKey[i], dataKey[i])
		assert.Equal(t, baseMapAll[baseKey[i]], dataMap[dataKey[i]])
	}

	return nil
}

//generate random bytes
const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const (
	letterIdxBits = 6                    // 6 bits to represent a letter index
	letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
	letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
)

func randomBytes(n int) []byte {
	b := make([]byte, n)
	// A rand.Int63() generates 63 random bits, enough for letterIdxMax letters!
	for i, cache, remain := n-1, rand.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = rand.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return b
}
