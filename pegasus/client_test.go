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

	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
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

	var tblist []TableConnector
	openTableQueries := 100

	var wg sync.WaitGroup
	for i := 0; i < openTableQueries; i++ {
		wg.Add(1)
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			tb, err := client.OpenTable(ctx, "temp")
			assert.Nil(t, err)
			tblist = append(tblist, tb)
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
			ctx, _ := context.WithTimeout(context.Background(), time.Second)
			_, err := client.OpenTable(ctx, "table_not_exist"+fmt.Sprint(id))
			assert.NotNil(t, err)
			wg.Done()
		}()
	}
	wg.Wait()
}

// Produce larger workload and test if anything goes wrong.
func TestPegasusClient_SequentialOperations(t *testing.T) {
	defer leaktest.CheckTimeout(t, time.Second*20)

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	for i := 0; i < 5000; i++ {
		ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
		hashKey := []byte(fmt.Sprintf("h%d", i))
		sortKey := []byte(fmt.Sprintf("s%d", i))
		value := []byte(fmt.Sprintf("v%d", i))

		err := client.Set(ctx, "temp", hashKey, sortKey, value)
		assert.Nil(t, err)

		actual, err := client.Get(ctx, "temp", hashKey, sortKey)
		assert.Nil(t, err)
		assert.Equal(t, actual, value)

		err = client.Del(ctx, "temp", hashKey, sortKey)
		assert.Nil(t, err)
	}
}

func TestPegasusClient_GetNotFound(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	value, err := tb.Get(context.Background(), []byte("h-notfound"), []byte("s-notfound"))
	assert.Nil(t, err)
	assert.Equal(t, []byte(nil), value)
}

func TestPegasusClient_ConcurrentDel(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	client.OpenTable(context.Background(), "temp")
	time.Sleep(time.Second)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)

		id := i
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
			hashKey := []byte(fmt.Sprintf("h%d", id))
			sortKey := []byte(fmt.Sprintf("s%d", id))

			err := client.Del(ctx, "temp", hashKey, sortKey)
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_ConcurrentSetAndDel(t *testing.T) {
	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)

		id := i
		go func() {
			ctx, _ := context.WithTimeout(context.Background(), time.Second*3)
			hashKey := []byte(fmt.Sprintf("h%d", id))
			sortKey := []byte(fmt.Sprintf("s%d", id))
			value := []byte(fmt.Sprintf("v%d", id))

			err := client.Set(ctx, "temp", hashKey, sortKey, value)
			assert.Nil(t, err)

			err = client.Del(ctx, "temp", hashKey, sortKey)
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_ConcurrentSetAndMultiGetRange(t *testing.T) {
	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	hashKey := []byte(fmt.Sprintf("h1"))

	///// clear up
	var wg sync.WaitGroup
	wg.Add(100)
	for i := 0; i < 100; i++ {
		id := i /// copy to prevent value changed while routine executed. It's necessary.
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			assert.Nil(t, client.Del(context.Background(), "temp", hashKey, sortKey))

			wg.Done()
		}()
	}
	wg.Wait()

	/// insert
	wg.Add(100)
	for i := 0; i < 100; i++ {
		id := i
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			value := []byte(fmt.Sprintf("v%d", id))

			err := client.Set(context.Background(), "temp", hashKey, sortKey, value)
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()

	/// get range
	wg.Add(8)
	for i := 1; i < 9; i++ {
		id := i
		go func() {
			startSortKey := []byte(fmt.Sprintf("s%d", id)) // start from i to i+1: s1 s10 s11 ... s2, in total 11 entries
			stopSortKey := []byte(fmt.Sprintf("s%d", id+1))
			options := MultiGetOptions{StartInclusive: true, StopInclusive: false}

			kvs, err := client.MultiGetRangeOpt(context.Background(), "temp", hashKey, startSortKey, stopSortKey, options)
			assert.Nil(t, err)
			assert.Equal(t, len(kvs), 11)

			for _, kv := range kvs {
				value, err := client.Get(context.Background(), "temp", hashKey, kv.SortKey)

				assert.Nil(t, err)
				assert.Equal(t, kv.Value, value)
			}

			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_ConcurrentSetAndMultiGet(t *testing.T) {
	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	hashKey := []byte(fmt.Sprintf("h1"))

	/// clear up
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)

		id := i
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			assert.Nil(t, client.Del(context.Background(), "temp", hashKey, sortKey))

			wg.Done()
		}()
	}
	wg.Wait()

	/// insert
	for i := 0; i < 10; i++ {
		wg.Add(1)

		id := i
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			value := []byte(fmt.Sprintf("v%d", id))

			err := client.Set(context.Background(), "temp", hashKey, sortKey, value)
			assert.Nil(t, err)

			wg.Done()
		}()
	}
	wg.Wait()

	/// get
	{
		wg.Add(1)

		go func() {
			sortKeys := make([][]byte, 10)
			for i := 0; i < 10; i++ {
				sortKeys[i] = []byte(fmt.Sprintf("s%d", i))
			}

			kvs, err := client.MultiGet(context.Background(), "temp", hashKey, sortKeys)
			assert.Nil(t, err)
			assert.Equal(t, len(kvs), 10)

			for i := 0; i < 10; i++ {
				assert.Equal(t, kvs[i].SortKey, []byte(fmt.Sprintf("s%d", i)))
				assert.Equal(t, kvs[i].Value, []byte(fmt.Sprintf("v%d", i)))
			}

			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_MultiSetAndMultiGet(t *testing.T) {
	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}

	client := NewClient(cfg)
	defer client.Close()

	hashKey := []byte(fmt.Sprintf("h1"))

	sortKeys := make([][]byte, 10)
	values := make([][]byte, 10)

	/// clear up
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)

		id := i
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			assert.Nil(t, client.Del(context.Background(), "temp", hashKey, sortKey))

			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		sortKeys[i] = []byte(fmt.Sprintf("s%d", i))
		values[i] = []byte(fmt.Sprintf("v%d", i))
	}

	assert.Nil(t, client.MultiSet(context.Background(), "temp", hashKey, sortKeys, values))

	/// get
	{
		wg.Add(1)

		go func() {
			kvs, err := client.MultiGet(context.Background(), "temp", hashKey, sortKeys)
			assert.Nil(t, err)
			assert.Equal(t, len(kvs), 10)

			for i := 0; i < 10; i++ {
				assert.Equal(t, kvs[i].SortKey, []byte(fmt.Sprintf("s%d", i)))
				assert.Equal(t, kvs[i].Value, []byte(fmt.Sprintf("v%d", i)))
			}

			wg.Done()
		}()
	}
	wg.Wait()
}

func TestPegasusClient_Exist(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)

	hashKey := []byte(fmt.Sprintf("h1"))

	sortKeys := make([][]byte, 10)
	values := make([][]byte, 10)

	/// clear up
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)

		id := i
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			assert.Nil(t, client.Del(context.Background(), "temp", hashKey, sortKey))

			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		sortKeys[i] = []byte(fmt.Sprintf("s%d", i))
		values[i] = []byte(fmt.Sprintf("v%d", i))
	}

	assert.Nil(t, client.MultiSetOpt(context.Background(), "temp", hashKey, sortKeys, values, 10))

	for i := 0; i < 10; i++ {
		exist, err := tb.Exist(context.Background(), hashKey, sortKeys[i])
		assert.Nil(t, err)
		assert.Equal(t, true, exist)
	}

}

func TestPegasusClient_TTL(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)

	hashKey := []byte(fmt.Sprintf("h1"))

	sortKeys := make([][]byte, 10)
	values := make([][]byte, 10)

	/// clear up
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)

		id := i
		go func() {
			sortKey := []byte(fmt.Sprintf("s%d", id))
			assert.Nil(t, client.Del(context.Background(), "temp", hashKey, sortKey))

			wg.Done()
		}()
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		sortKeys[i] = []byte(fmt.Sprintf("s%d", i))
		values[i] = []byte(fmt.Sprintf("v%d", i))
	}

	assert.Nil(t, client.MultiSetOpt(context.Background(), "temp", hashKey, sortKeys, values, 10))
	for i := 0; i < 10; i++ {
		exist, err := tb.Exist(context.Background(), hashKey, sortKeys[0])
		assert.Nil(t, err)
		assert.Equal(t, true, exist)
	}

	time.Sleep(time.Second * 11)

	// after more than 10s, keys should be not found.
	for i := 0; i < 10; i++ {
		ttl, _ := tb.TTL(context.Background(), hashKey, sortKeys[i])
		assert.Equal(t, -2, ttl)
	}
}

func TestPegasusClient_ScanAllSortKey(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	options := ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}
	scanner, err := client.GetScanner(context.Background(), "temp", []byte("h1"), []byte{}, []byte{}, options)
	assert.Nil(t, err)

	dataMap := make(map[string]string)
	for {
		err, completed, h, s, v := scanner.Next(context.Background())
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := dataMap[string(s)]
		assert.False(t, ok)
		dataMap[string(s)] = string(v)
	}
	scanner.Close()
	compareMaps(t, dataMap, baseMap["h1"])
}

func TestPegasusClient_ScanInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	var start, stop []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}
	for s := range baseMap["h1"] {
		stop = []byte(s)
		break
	}
	if string(start) > string(stop) {
		temp := stop
		stop = start
		start = temp
	}

	options := ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		StopInclusive:  true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}

	scanner, err := client.GetScanner(context.Background(), "temp", []byte("h1"), start, stop, options)
	assert.Nil(t, err)

	dataMap := make(map[string]string)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for {
		err, completed, h, s, v := scanner.Next(ctx)
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := dataMap[string(s)]
		assert.False(t, ok)
		dataMap[string(s)] = string(v)
	}
	scanner.Close()

	cutAndCompareMaps(t, dataMap, baseMap["h1"], start, true, stop, true)
}

func TestPegasusClient_ScanExclusive(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	var start, stop []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}
	for s := range baseMap["h1"] {
		if s == string(start) {
			continue
		}
		stop = []byte(s)
		break
	}
	if string(start) > string(stop) {
		temp := stop
		stop = start
		start = temp
	}

	options := ScannerOptions{
		BatchSize:      1000,
		StartInclusive: false,
		StopInclusive:  false,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}

	scanner, err := client.GetScanner(context.Background(), "temp", []byte("h1"), start, stop, options)
	assert.Nil(t, err)
	assert.NotNil(t, scanner)
	dataMap := make(map[string]string)
	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for {
		err, completed, h, s, v := scanner.Next(ctx)
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := dataMap[string(s)]
		assert.False(t, ok)
		dataMap[string(s)] = string(v)
	}
	scanner.Close()

	err = cutAndCompareMaps(t, dataMap, baseMap["h1"], start, false, stop, false)
	//fmt.Println(err.Error()) if can't cut the baseMap, abandon compare
}

func TestPegasusClient_ScanOnePoint(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	var start []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = true
	scanner, err := client.GetScanner(context.Background(), "temp", []byte("h1"), start, start, options)
	assert.Nil(t, err)
	err, completed, h, s, v := scanner.Next(context.Background())
	assert.Nil(t, err)
	assert.False(t, completed)
	assert.Equal(t, []byte("h1"), h)
	assert.Equal(t, start, s)
	assert.Equal(t, baseMap["h1"][string(start)], string(v))

	err, completed, _, _, _ = scanner.Next(context.Background())
	assert.Nil(t, err)
	assert.True(t, completed)
	scanner.Close()
}

func TestPegasusClient_ScanHalfInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	var start []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = false
	_, err := client.GetScanner(context.Background(), "temp", []byte("h1"), start, start, options)
	assert.NotNil(t, err)
}

func TestPegasusClient_ScanVoidSpan(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	var start, stop []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}
	for s := range baseMap["h1"] {
		if s == string(start) {
			continue
		}
		stop = []byte(s)
		break
	}
	if string(start) > string(stop) {
		temp := stop
		stop = start
		start = temp
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = true
	_, err := client.GetScanner(context.Background(), "temp", []byte("h1"), stop, start, options)
	assert.NotNil(t, err)
}

func TestPegasusClient_ScanOverallScan(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	options := NewScanOptions()
	dataMap := make(map[string]string)

	scanners, err := client.GetUnorderedScanners(context.Background(), "temp", 3, options)
	assert.Nil(t, err)
	assert.True(t, len(scanners) <= 3)

	for _, s := range scanners {
		assert.NotNil(t, s)
		for {
			err, completed, h, s, v := s.Next(context.Background())
			assert.Nil(t, err)
			if completed {
				break
			}

			blob := encodeHashKeySortKey(h, s)
			dataMap[string(blob.Data)] = string(v)
		}
		s.Close()
	}

	compareAll(t, dataMap, baseMap)
}

func TestPegasusClient_ConcurrentCallScanner(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	batchSizes := []int{10, 100, 500, 1000}

	var wg sync.WaitGroup
	for i := 0; i < len(batchSizes); i++ {
		wg.Add(1)
		//go func(i int) {
		batchSize := batchSizes[i]
		options := NewScanOptions()
		options.BatchSize = batchSize

		dataMap := make(map[string]string)
		scanners, err := client.GetUnorderedScanners(context.Background(), "temp", 1, options)
		assert.Nil(t, err)
		assert.True(t, len(scanners) <= 1)

		scanner := scanners[0]
		for {
			err, completed, h, s, v := scanner.Next(context.Background())
			assert.Nil(t, err)
			if completed {
				break
			}
			blob := encodeHashKeySortKey(h, s)
			dataMap[string(blob.Data)] = string(v)
		}
		scanner.Close()
		compareAll(t, dataMap, baseMap)
		wg.Done()
		//}(i)
	}
	wg.Wait()
}

func TestPegasusClient_NoValueScan(t *testing.T) {
	defer leaktest.Check(t)()

	cfg := Config{
		MetaServers: []string{"0.0.0.0:34601", "0.0.0.0:34602", "0.0.0.0:34603"},
	}
	client := NewClient(cfg)
	defer client.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, client)
	setDatabase(client, baseMap)

	options := ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}
	options.NoValue = true
	scanner, err := client.GetScanner(context.Background(), "temp", []byte("h1"), []byte{}, []byte{}, options)
	assert.Nil(t, err)

	ctx, _ := context.WithTimeout(context.Background(), time.Second*5)
	for {
		err, completed, h, s, v := scanner.Next(ctx)
		assert.Nil(t, err)
		if completed {
			break
		}
		assert.Equal(t, []byte("h1"), h)
		_, ok := baseMap["h1"][string(s)]
		assert.True(t, ok)
		assert.True(t, len(v) == 0)
	}
	scanner.Close()
}

func clearDatabase(t *testing.T, client Client) {
	options := NewScanOptions()
	scanners, err := client.GetUnorderedScanners(context.Background(), "temp", 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])

	for {
		err1, completed, h, s, _ := scanners[0].Next(context.Background())
		assert.Nil(t, err1)
		if completed {
			break
		}
		err = client.Del(context.Background(), "temp", h, s)
		assert.Nil(t, err)
	}

	scanners[0].Close()

	scanners, err = client.GetUnorderedScanners(context.Background(), "temp", 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])
	err, completed, _, _, _ := scanners[0].Next(context.Background())
	assert.Nil(t, err)
	assert.True(t, completed)
}

func setDatabase(client Client, baseMap map[string]map[string]string) {
	hashMap := make(map[string]string)
	for i := 0; i < 10 || len(hashMap) < 10; i++ {
		s := randomBytes(100)
		v := randomBytes(100)
		client.Set(context.Background(), "temp", []byte("h1"), s, v)
		hashMap[string(s)] = string(v)
	}
	baseMap["h1"] = hashMap

	for i := 0; i < 100 || len(baseMap) < 100; i++ {
		h := randomBytes(100)
		sortMap, ok := baseMap[string(h)]
		if !ok {
			sortMap = make(map[string]string)
			baseMap[string(h)] = sortMap
		}
		for j := 0; j < 10 || len(sortMap) < 10; j++ {
			s := randomBytes(100)
			v := randomBytes(100)
			client.Set(context.Background(), "temp", h, s, v)
			sortMap[string(s)] = string(v)
		}

	}
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
