package pegasus

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

func clearDatabase(t *testing.T, tb TableConnector) {
	options := NewScanOptions()
	scanners, err := tb.GetUnorderedScanners(context.Background(), 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])

	for {
		completed, h, s, _, err1 := scanners[0].Next(context.Background())
		assert.Nil(t, err1)
		if completed {
			break
		}
		err = tb.Del(context.Background(), h, s)
		assert.Nil(t, err)
	}

	scanners[0].Close()

	scanners, err = tb.GetUnorderedScanners(context.Background(), 1, options)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(scanners))
	assert.NotNil(t, scanners[0])
	completed, _, _, _, err := scanners[0].Next(context.Background())
	assert.Nil(t, err)
	assert.True(t, completed)
}

func setDatabase(tb TableConnector, baseMap map[string]map[string]string) {
	hashMap := make(map[string]string)
	for i := 0; i < 10 || len(hashMap) < 10; i++ {
		s := randomBytes(100)
		v := randomBytes(100)
		tb.Set(context.Background(), []byte("h1"), s, v)
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
			tb.Set(context.Background(), h, s, v)
			sortMap[string(s)] = string(v)
		}

	}
}

func TestPegasusTableConnector_ConcurrentCallScanner(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	batchSizes := []int{10, 100, 500, 1000}

	var wg sync.WaitGroup
	for i := 0; i < len(batchSizes); i++ {
		wg.Add(1)
		batchSize := batchSizes[i]
		options := NewScanOptions()
		options.BatchSize = batchSize

		dataMap := make(map[string]string)
		scanners, err := tb.GetUnorderedScanners(context.Background(), 1, options)
		assert.Nil(t, err)
		assert.True(t, len(scanners) <= 1)

		scanner := scanners[0]
		for {
			completed, h, s, v, err := scanner.Next(context.Background())
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
	}
	wg.Wait()
}

func TestPegasusTableConnector_NoValueScan(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}
	options.NoValue = true
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), []byte{}, []byte{}, options)
	assert.Nil(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for {
		completed, h, s, v, err := scanner.Next(ctx)
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

func TestPegasusTableConnector_ScanAllSortKey(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), []byte{}, []byte{}, options)
	assert.Nil(t, err)

	dataMap := make(map[string]string)
	for {
		completed, h, s, v, err := scanner.Next(context.Background())
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

func TestPegasusTableConnector_ScanInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

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

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		StopInclusive:  true,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}

	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), start, stop, options)
	assert.Nil(t, err)

	dataMap := make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for {
		completed, h, s, v, err := scanner.Next(ctx)
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

func TestPegasusTableConnector_ScanExclusive(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

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

	options := &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: false,
		StopInclusive:  false,
		HashKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
		SortKeyFilter:  Filter{FilterTypeMatchPrefix, []byte("")},
	}

	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), start, stop, options)
	assert.Nil(t, err)
	assert.NotNil(t, scanner)
	dataMap := make(map[string]string)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	for {
		completed, h, s, v, err2 := scanner.Next(ctx)
		assert.Nil(t, err2)
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
	assert.Nil(t, err)
}

func TestPegasusTableConnector_ScanOnePoint(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = true
	scanner, err := tb.GetScanner(context.Background(), []byte("h1"), start, start, options)
	assert.Nil(t, err)
	completed, h, s, v, err := scanner.Next(context.Background())
	assert.Nil(t, err)
	assert.False(t, completed)
	assert.Equal(t, []byte("h1"), h)
	assert.Equal(t, start, s)
	assert.Equal(t, baseMap["h1"][string(start)], string(v))

	completed, _, _, _, err = scanner.Next(context.Background())
	assert.Nil(t, err)
	assert.True(t, completed)
	scanner.Close()
}

func TestPegasusTableConnector_ScanHalfInclusive(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	var start []byte
	for s := range baseMap["h1"] {
		start = []byte(s)
		break
	}

	options := NewScanOptions()
	options.StartInclusive = true
	options.StopInclusive = false
	_, err = tb.GetScanner(context.Background(), []byte("h1"), start, start, options)
	assert.NotNil(t, err)
}

func TestPegasusTableConnector_ScanVoidSpan(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

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
	_, err = tb.GetScanner(context.Background(), []byte("h1"), stop, start, options)
	assert.NotNil(t, err)
}

func TestPegasusTableConnector_ScanOverallScan(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	baseMap := make(map[string]map[string]string)
	clearDatabase(t, tb)
	setDatabase(tb, baseMap)

	options := NewScanOptions()
	dataMap := make(map[string]string)

	scanners, err := tb.GetUnorderedScanners(context.Background(), 3, options)
	assert.Nil(t, err)
	assert.True(t, len(scanners) <= 3)

	for _, s := range scanners {
		assert.NotNil(t, s)
		for {
			completed, h, s, v, err := s.Next(context.Background())
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

func setDatabase2(tb TableConnector) {
	var start int64 = 1611331200 // 2021-01-23 00:00:00
	var end int64 = 1611676800   // 2021-01-27 00:00:00
	// Insert each minute timeString into DB
	for timeStamp := start; timeStamp < end; timeStamp += 60 {
		timeNow := time.Unix(timeStamp, 0)
		timeString := timeNow.Format("2006-01-02 15:04:05")
		tb.Set(context.Background(), []byte(timeString), []byte("cu"), []byte("fortest"))
	}
}

func TestPegasusTableConnector_ScanWithFilter(t *testing.T) {
	defer leaktest.Check(t)()

	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	clearDatabase(t, tb)
	setDatabase2(tb)

	sopts := &ScannerOptions{
		BatchSize:     5,
		HashKeyFilter: Filter{Type: FilterTypeMatchAnywhere, Pattern: []byte("2021-01-25")},
	}
	scanners, _ := tb.GetUnorderedScanners(context.Background(), 256, sopts)

	minutePerDay := 0
	for _, scanner := range scanners {
		for {
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute*10)
			defer cancel()
			completed, _, _, _, err := scanner.Next(ctx)
			if err != nil {
				return
			}
			if completed {
				break
			}
			minutePerDay++
		}
	}
	assert.True(t, minutePerDay == 1440)
}
