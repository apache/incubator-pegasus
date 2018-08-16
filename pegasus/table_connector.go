// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/XiaoMi/pegasus-go-client/session"
	"gopkg.in/tomb.v2"
)

// KeyValue is the returned type of MultiGet and MultiGetRange.
type KeyValue struct {
	SortKey, Value []byte
}

// MultiGetOptions is the options for MultiGet and MultiGetRange, defaults to DefaultMultiGetOptions.
type MultiGetOptions struct {
	StartInclusive bool
	StopInclusive  bool
	SortKeyFilter  Filter

	// MaxFetchCount and MaxFetchSize limit the size of returned result.

	// Max count of k-v pairs to be fetched. MaxFetchCount <= 0 means no limit.
	MaxFetchCount int

	// Max size of k-v pairs to be fetched. MaxFetchSize <= 0 means no limit.
	MaxFetchSize int

	// Query order
	Reverse bool
}

// DefaultMultiGetOptions defines the defaults of MultiGetOptions.
var DefaultMultiGetOptions = &MultiGetOptions{
	StartInclusive: true,
	StopInclusive:  false,
	SortKeyFilter: Filter{
		Type:    FilterTypeNoFilter,
		Pattern: nil,
	},
	MaxFetchCount: 100,
	MaxFetchSize:  100000,
}

// TableConnector is used to communicate with single Pegasus table.
type TableConnector interface {
	// Get retrieves the entry for `hashKey` + `sortKey`.
	// Returns nil if no entry matches.
	// `hashKey` : CAN'T be nil or empty.
	// `sortKey` : CAN'T be nil but CAN be empty.
	Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error)

	// Set the entry for `hashKey` + `sortKey` to `value`.
	// If Set is called or `ttl` == 0, no data expiration is specified.
	// `hashKey` : CAN'T be nil or empty.
	// `sortKey` / `value` : CAN'T be nil but CAN be empty.
	Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error
	SetTTL(ctx context.Context, hashKey []byte, sortKey []byte, value []byte, ttl time.Duration) error

	// Delete the entry for `hashKey` + `sortKey`.
	// `hashKey` : CAN'T be nil or empty.
	// `sortKey` : CAN'T be nil but CAN be empty.
	Del(ctx context.Context, hashKey []byte, sortKey []byte) error

	// MultiGet/MultiGetOpt retrieves the multiple entries for `hashKey` + `sortKeys[i]` atomically in one operation.
	// MultiGet is identical to MultiGetOpt except that the former uses DefaultMultiGetOptions as `options`.
	//
	// If `sortKeys` are given empty or nil, all entries under `hashKey` will be retrieved.
	// `hashKey` : CAN'T be nil or empty.
	// `sortKeys[i]` : CAN'T be nil but CAN be empty.
	//
	// The returned key-value pairs are sorted by sort key in ascending order.
	// Returns nil if no entries match.
	// Returns true if all data is fetched, false if only partial data is fetched.
	//
	MultiGet(ctx context.Context, hashKey []byte, sortKeys [][]byte) ([]*KeyValue, bool, error)
	MultiGetOpt(ctx context.Context, hashKey []byte, sortKeys [][]byte, options *MultiGetOptions) ([]*KeyValue, bool, error)

	// MultiGetRange retrieves the multiple entries under `hashKey`, between range (`startSortKey`, `stopSortKey`),
	// atomically in one operation.
	//
	// startSortKey: nil or len(startSortKey) == 0 means start from begin.
	// stopSortKey: nil or len(stopSortKey) == 0 means stop to end.
	// `hashKey` : CAN'T be nil.
	//
	// The returned key-value pairs are sorted by sort keys in ascending order.
	// Returns nil if no entries match.
	// Returns true if all data is fetched, false if only partial data is fetched.
	//
	MultiGetRange(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte) ([]*KeyValue, bool, error)
	MultiGetRangeOpt(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options *MultiGetOptions) ([]*KeyValue, bool, error)

	// MultiSet sets the multiple entries for `hashKey` + `sortKeys[i]` atomically in one operation.
	// `hashKey` / `sortKeys` / `values` : CAN'T be nil or empty.
	// `sortKeys[i]` / `values[i]` : CAN'T be nil but CAN be empty.
	MultiSet(ctx context.Context, hashKey []byte, sortKeys [][]byte, values [][]byte) error
	MultiSetOpt(ctx context.Context, hashKey []byte, sortKeys [][]byte, values [][]byte, ttl time.Duration) error

	// MultiDel deletes the multiple entries under `hashKey` all in one operation.
	// `hashKey` / `sortKeys` : CAN'T be nil or empty.
	// `sortKeys[i]` : CAN'T be nil but CAN be empty.
	MultiDel(ctx context.Context, hashKey []byte, sortKeys [][]byte) error

	// Returns ttl(time-to-live) in seconds: -1 if ttl is not set; -2 if entry doesn't exist.
	// `hashKey` : CAN'T be nil or empty.
	// `sortKey` : CAN'T be nil but CAN be empty.
	TTL(ctx context.Context, hashKey []byte, sortKey []byte) (int, error)

	// Check value existence for the entry for `hashKey` + `sortKey`.
	// `hashKey`: CAN'T be nil or empty.
	Exist(ctx context.Context, hashKey []byte, sortKey []byte) (bool, error)

	// Get Scanner for {startSortKey, stopSortKey} within hashKey.
	// startSortKey: nil or len(startSortKey) == 0 means start from begin.
	// stopSortKey: nil or len(stopSortKey) == 0 means stop to end.
	// `hashKey`: CAN'T be nil or empty.
	GetScanner(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options *ScannerOptions) (Scanner, error)

	// Get Scanners for all data in pegasus, the count of scanners will
	// be no more than maxSplitCount
	GetUnorderedScanners(ctx context.Context, maxSplitCount int, options *ScannerOptions) ([]Scanner, error)

	// Atomically check and set value by key from the cluster. The value will be set if and only if check passed.
	// The sort key for checking and setting can be the same or different.
	//
	// `checkSortKey`: The sort key for checking.
	// `setSortKey`: The sort key for setting.
	// `checkOperand`:
	CheckAndSet(ctx context.Context, hashKey []byte, checkSortKey []byte, checkType CheckType,
		checkOperand []byte, setSortKey []byte, setValue []byte, options *CheckAndSetOptions) (*CheckAndSetResult, error)

	// Returns the count of sortkeys under hashkey.
	// `hashKey`: CAN'T be nil or empty.
	SortKeyCount(ctx context.Context, hashKey []byte) (int64, error)

	Close() error
}

type pegasusTableConnector struct {
	meta    *session.MetaManager
	replica *session.ReplicaManager

	logger pegalog.Logger

	tableName string
	appID     int32
	parts     []*replicaNode
	mu        sync.RWMutex

	confUpdateCh chan bool
	tom          tomb.Tomb
}

type replicaNode struct {
	session *session.ReplicaSession
	pconf   *replication.PartitionConfiguration
}

// ConnectTable queries for the configuration of the given table, and set up connection to
// the replicas which the table locates on.
func ConnectTable(ctx context.Context, tableName string, meta *session.MetaManager, replica *session.ReplicaManager) (TableConnector, error) {
	p := &pegasusTableConnector{
		tableName:    tableName,
		meta:         meta,
		replica:      replica,
		confUpdateCh: make(chan bool, 1),
		logger:       pegalog.GetLogger(),
	}

	if err := p.updateConf(ctx); err != nil {
		return nil, err
	}

	p.tom.Go(p.loopForAutoUpdate)
	return p, nil
}

// Update configuration of this table.
func (p *pegasusTableConnector) updateConf(ctx context.Context) error {
	resp, err := p.meta.QueryConfig(ctx, p.tableName)
	if err == nil {
		err = p.handleQueryConfigResp(resp)
	}
	if err != nil {
		return fmt.Errorf("failed to connect table(%s): %s", p.tableName, err)
	}
	return nil
}

func (p *pegasusTableConnector) handleQueryConfigResp(resp *replication.QueryCfgResponse) error {
	if resp.Err.Errno != base.ERR_OK.String() {
		return errors.New(resp.Err.Errno)
	}
	if resp.PartitionCount == 0 || len(resp.Partitions) != int(resp.PartitionCount) {
		return fmt.Errorf("invalid table configuration: response [%v]", resp)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	p.appID = resp.AppID

	if len(resp.Partitions) > len(p.parts) {
		// during partition split or first configuration update of client.
		for _, part := range p.parts {
			part.session.Close()
		}
		p.parts = make([]*replicaNode, len(resp.Partitions))
	}

	// TODO(wutao1): make sure PartitionIndex are continuous
	for _, pconf := range resp.Partitions {
		if pconf == nil || pconf.Primary == nil || pconf.Primary.GetRawAddress() == 0 {
			return fmt.Errorf("unable to resolve routing table [appid: %d]: [%v]", p.appID, pconf)
		}
		r := &replicaNode{
			pconf:   pconf,
			session: p.replica.GetReplica(pconf.Primary.GetAddress()),
		}
		p.parts[pconf.Pid.PartitionIndex] = r
	}
	return nil
}

func validateHashKey(hashKey []byte) error {
	if hashKey == nil {
		return fmt.Errorf("InvalidParameter: hashkey must not be nil")
	}
	if len(hashKey) == 0 {
		return fmt.Errorf("InvalidParameter: hashkey must not be empty")
	}
	if len(hashKey) > math.MaxUint16 {
		return fmt.Errorf("InvalidParameter: length of hashkey (%d) must be less than %d", len(hashKey), math.MaxUint16)
	}
	return nil
}

func validateValue(value []byte) error {
	if value == nil {
		return fmt.Errorf("InvalidParameter: value must not be nil")
	}
	return nil
}

func validateValues(values [][]byte) error {
	if values == nil {
		return fmt.Errorf("InvalidParameter: values must not be nil")
	}
	if len(values) == 0 {
		return fmt.Errorf("InvalidParameter: values must not be empty")
	}
	for i, value := range values {
		if value == nil {
			return fmt.Errorf("InvalidParameter: values[%d] must not be nil", i)
		}
	}
	return nil
}

func validateSortKey(sortKey []byte) error {
	if sortKey == nil {
		return fmt.Errorf("InvalidParameter: sortkey must not be nil")
	}
	return nil
}

func validateSortKeys(sortKeys [][]byte) error {
	if sortKeys == nil {
		return fmt.Errorf("InvalidParameter: sortkeys must not be nil")
	}
	if len(sortKeys) == 0 {
		return fmt.Errorf("InvalidParameter: sortkeys must not be empty")
	}
	for i, sortKey := range sortKeys {
		if sortKey == nil {
			return fmt.Errorf("InvalidParameter: sortkeys[%d] must not be nil", i)
		}
	}
	return nil
}

// WrapError wraps up the internal errors for ensuring that all types of errors
// returned by public interfaces are pegasus.PError.
func WrapError(err error, op OpType) error {
	if err != nil {
		if pe, ok := err.(*PError); ok {
			pe.Op = op
			return pe
		}
		return &PError{
			Err: err,
			Op:  op,
		}
	}
	return nil
}

func (p *pegasusTableConnector) Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error) {
	b, err := func() ([]byte, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, err
		}
		if err := validateSortKey(sortKey); err != nil {
			return nil, err
		}

		key := encodeHashKeySortKey(hashKey, sortKey)
		gpid, part := p.getPartition(hashKey)

		resp, err := part.Get(ctx, gpid, key)
		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		if err == base.NotFound {
			// Success for non-existed entry.
			return nil, nil
		}
		if err = p.handleReplicaError(err, gpid, part); err != nil {
			return nil, err
		}
		return resp.Value.Data, nil
	}()
	return b, WrapError(err, OpGet)
}

func (p *pegasusTableConnector) SetTTL(ctx context.Context, hashKey []byte, sortKey []byte, value []byte, ttl time.Duration) error {
	err := func() error {
		if err := validateHashKey(hashKey); err != nil {
			return err
		}
		if err := validateSortKey(sortKey); err != nil {
			return err
		}
		if err := validateValue(value); err != nil {
			return err
		}

		key := encodeHashKeySortKey(hashKey, sortKey)
		val := &base.Blob{Data: value}
		gpid, part := p.getPartition(hashKey)
		expireTsSec := int32(0)
		if ttl != 0 {
			expireTsSec = expireTsSeconds(ttl)
		}

		resp, err := part.Put(ctx, gpid, key, val, expireTsSec)
		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		return p.handleReplicaError(err, gpid, part)
	}()
	return WrapError(err, OpSet)
}

func (p *pegasusTableConnector) Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error {
	return p.SetTTL(ctx, hashKey, sortKey, value, 0)
}

func (p *pegasusTableConnector) Del(ctx context.Context, hashKey []byte, sortKey []byte) error {
	err := func() error {
		if err := validateHashKey(hashKey); err != nil {
			return err
		}
		if err := validateSortKey(sortKey); err != nil {
			return err
		}

		key := encodeHashKeySortKey(hashKey, sortKey)
		gpid, part := p.getPartition(hashKey)

		resp, err := part.Del(ctx, gpid, key)
		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		return p.handleReplicaError(err, gpid, part)
	}()
	return WrapError(err, OpDel)
}

func setRequestByOption(options *MultiGetOptions, request *rrdb.MultiGetRequest) {
	request.MaxKvCount = int32(options.MaxFetchCount)
	request.MaxKvSize = int32(options.MaxFetchSize)
	request.StartInclusive = options.StartInclusive
	request.StopInclusive = options.StopInclusive
	request.SortKeyFilterType = rrdb.FilterType(options.SortKeyFilter.Type)
	request.SortKeyFilterPattern = &base.Blob{Data: options.SortKeyFilter.Pattern}
	request.Reverse = options.Reverse
}

func (p *pegasusTableConnector) MultiGetOpt(ctx context.Context, hashKey []byte, sortKeys [][]byte, options *MultiGetOptions) ([]*KeyValue, bool, error) {
	kvs, allFetched, err := func() ([]*KeyValue, bool, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, false, err
		}
		if len(sortKeys) != 0 {
			// sortKeys are nil-able, nil means fetching all entries.
			if err := validateSortKeys(sortKeys); err != nil {
				return nil, false, err
			}
		}

		request := rrdb.NewMultiGetRequest()
		request.HashKey = &base.Blob{Data: hashKey}
		request.SorkKeys = make([]*base.Blob, len(sortKeys))
		request.StartSortkey = &base.Blob{}
		request.StopSortkey = &base.Blob{}
		for i, sortKey := range sortKeys {
			request.SorkKeys[i] = &base.Blob{Data: sortKey}
		}
		setRequestByOption(options, request)

		return p.doMultiGet(ctx, hashKey, request)
	}()
	return kvs, allFetched, WrapError(err, OpMultiGet)
}

func (p *pegasusTableConnector) MultiGet(ctx context.Context, hashKey []byte, sortKeys [][]byte) ([]*KeyValue, bool, error) {
	return p.MultiGetOpt(ctx, hashKey, sortKeys, DefaultMultiGetOptions)
}

func (p *pegasusTableConnector) MultiGetRangeOpt(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options *MultiGetOptions) ([]*KeyValue, bool, error) {
	kvs, allFetched, err := func() ([]*KeyValue, bool, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, false, err
		}

		request := rrdb.NewMultiGetRequest()
		request.HashKey = &base.Blob{Data: hashKey}
		request.StartSortkey = &base.Blob{Data: startSortKey}
		request.StopSortkey = &base.Blob{Data: stopSortKey}
		setRequestByOption(options, request)

		return p.doMultiGet(ctx, hashKey, request)
	}()
	return kvs, allFetched, WrapError(err, OpMultiGetRange)
}

func (p *pegasusTableConnector) MultiGetRange(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte) ([]*KeyValue, bool, error) {
	return p.MultiGetRangeOpt(ctx, hashKey, startSortKey, stopSortKey, DefaultMultiGetOptions)
}

func (p *pegasusTableConnector) doMultiGet(ctx context.Context, hashKey []byte, request *rrdb.MultiGetRequest) ([]*KeyValue, bool, error) {
	gpid, part := p.getPartition(hashKey)
	resp, err := part.MultiGet(ctx, gpid, request)

	if err == nil {
		err = base.NewRocksDBErrFromInt(resp.Error)
	}

	allFetched := true
	if err == base.Incomplete {
		// partial data is fetched
		allFetched = false
		err = nil
	}
	if err = p.handleReplicaError(err, gpid, part); err == nil {
		if len(resp.Kvs) == 0 {
			return nil, allFetched, nil
		}
		kvs := make([]*KeyValue, len(resp.Kvs))
		for i, blobKv := range resp.Kvs {
			kvs[i] = &KeyValue{
				SortKey: blobKv.Key.Data,
				Value:   blobKv.Value.Data,
			}
		}

		sort.Slice(kvs, func(i, j int) bool {
			return bytes.Compare(kvs[i].SortKey, kvs[j].SortKey) < 0
		})

		return kvs, allFetched, nil
	}
	return nil, false, err
}

func (p *pegasusTableConnector) MultiSet(ctx context.Context, hashKey []byte, sortKeys [][]byte, values [][]byte) error {
	return p.MultiSetOpt(ctx, hashKey, sortKeys, values, 0)
}

func (p *pegasusTableConnector) MultiSetOpt(ctx context.Context, hashKey []byte, sortKeys [][]byte, values [][]byte, ttl time.Duration) error {
	err := func() error {
		if err := validateHashKey(hashKey); err != nil {
			return err
		}
		if err := validateSortKeys(sortKeys); err != nil {
			return err
		}
		if err := validateValues(values); err != nil {
			return err
		}
		if len(sortKeys) != len(values) {
			return fmt.Errorf("InvalidParameter: unmatched key-value pairs: len(sortKeys)=%d len(values)=%d",
				len(sortKeys), len(values))
		}

		request := rrdb.NewMultiPutRequest()
		request.HashKey = &base.Blob{Data: hashKey}
		request.Kvs = make([]*rrdb.KeyValue, len(sortKeys))
		for i := 0; i < len(sortKeys); i++ {
			request.Kvs[i] = &rrdb.KeyValue{
				Key:   &base.Blob{Data: sortKeys[i]},
				Value: &base.Blob{Data: values[i]},
			}
		}
		request.ExpireTsSeconds = 0
		if ttl != 0 {
			request.ExpireTsSeconds = expireTsSeconds(ttl)
		}

		return p.doMultiSet(ctx, hashKey, request)
	}()
	return WrapError(err, OpMultiSet)
}

func (p *pegasusTableConnector) doMultiSet(ctx context.Context, hashKey []byte, request *rrdb.MultiPutRequest) error {

	gpid, part := p.getPartition(hashKey)
	resp, err := part.MultiSet(ctx, gpid, request)

	if err == nil {
		err = base.NewRocksDBErrFromInt(resp.Error)
	}

	if err = p.handleReplicaError(err, gpid, part); err == nil {
		return nil
	}
	return err
}

func (p *pegasusTableConnector) MultiDel(ctx context.Context, hashKey []byte, sortKeys [][]byte) error {
	err := func() error {
		if err := validateHashKey(hashKey); err != nil {
			return err
		}
		if err := validateSortKeys(sortKeys); err != nil {
			return err
		}

		gpid, part := p.getPartition(hashKey)

		request := rrdb.NewMultiRemoveRequest()
		request.HashKey = &base.Blob{Data: hashKey}
		request.SorkKeys = make([]*base.Blob, len(sortKeys))
		for i, sortKey := range sortKeys {
			request.SorkKeys[i] = &base.Blob{Data: sortKey}
		}

		resp, err := part.MultiDelete(ctx, gpid, request)

		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		return p.handleReplicaError(err, gpid, part)
	}()
	return WrapError(err, OpMultiDel)
}

func (p *pegasusTableConnector) doTTL(ctx context.Context, hashKey []byte, sortKey []byte) (int, error) {
	if err := validateHashKey(hashKey); err != nil {
		return 0, err
	}
	if err := validateSortKey(sortKey); err != nil {
		return 0, err
	}

	key := encodeHashKeySortKey(hashKey, sortKey)
	gpid, part := p.getPartition(hashKey)

	resp, err := part.TTL(ctx, gpid, key)
	if err == nil {
		err = base.NewRocksDBErrFromInt(resp.Error)
		if err == base.NotFound {
			return -2, nil
		}
	}
	if err = p.handleReplicaError(err, gpid, part); err != nil {
		return -2, err
	}
	return int(resp.GetTTLSeconds()), nil
}

// -2 means entry not found.
func (p *pegasusTableConnector) TTL(ctx context.Context, hashKey []byte, sortKey []byte) (int, error) {
	ttl, err := p.doTTL(ctx, hashKey, sortKey)
	return ttl, WrapError(err, OpTTL)
}

func (p *pegasusTableConnector) Exist(ctx context.Context, hashKey []byte, sortKey []byte) (bool, error) {
	ttl, err := p.doTTL(ctx, hashKey, sortKey)

	if err == nil {
		if ttl == -2 {
			return false, nil
		}
		return true, nil
	}
	return false, WrapError(err, OpExist)
}

func (p *pegasusTableConnector) GetScanner(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte,
	options *ScannerOptions) (Scanner, error) {
	scanner, err := func() (Scanner, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, err
		}

		start := encodeHashKeySortKey(hashKey, startSortKey)
		var stop *base.Blob
		if len(stopSortKey) == 0 {
			stop = encodeHashKeySortKey(hashKey, []byte{0xFF, 0xFF}) // []byte{0xFF, 0xFF} means the max sortKey value
			options.StopInclusive = false
		} else {
			stop = encodeHashKeySortKey(hashKey, stopSortKey)
		}

		if options.SortKeyFilter.Type == FilterTypeMatchPrefix {
			prefixStartBlob := encodeHashKeySortKey(hashKey, options.SortKeyFilter.Pattern)

			// if the prefixStartKey generated by pattern is greater than the startKey, start from the prefixStartKey
			if bytes.Compare(prefixStartBlob.Data, start.Data) > 0 {
				start = prefixStartBlob
				options.StartInclusive = true
			}

			prefixStop := encodeNextBytesByKeys(hashKey, options.SortKeyFilter.Pattern)

			// if the prefixStopKey generated by pattern is less than the stopKey, end to the prefixStopKey
			if bytes.Compare(prefixStop.Data, stop.Data) <= 0 {
				stop = prefixStop
				options.StopInclusive = false
			}
		}

		cmp := bytes.Compare(start.Data, stop.Data)
		if cmp < 0 || (cmp == 0 && options.StartInclusive && options.StopInclusive) {
			gpid, err := p.getGpid(start.Data)
			if err != nil && gpid != nil {
				return nil, err
			}
			return newPegasusScanner(p, gpid, options, start, stop), nil
		}
		return nil, fmt.Errorf("the scanning interval MUST NOT BE EMPTY")
	}()
	return scanner, WrapError(err, OpGetScanner)
}

func (p *pegasusTableConnector) GetUnorderedScanners(ctx context.Context, maxSplitCount int,
	options *ScannerOptions) ([]Scanner, error) {
	scanners, err := func() ([]Scanner, error) {
		if maxSplitCount <= 0 {
			panic(fmt.Sprintf("invalid maxSplitCount: %d", maxSplitCount))
		}
		allGpid := p.getAllGpid()
		total := len(allGpid)

		var split int // the actual split count
		if total < maxSplitCount {
			split = total
		} else {
			split = maxSplitCount
		}
		scanners := make([]Scanner, split)

		// k: the smallest multiple of split which is greater than or equal to total
		k := 1
		for ; k*split < total; k++ {
		}
		left := total - k*(split-1)

		sliceLen := 0
		id := 0
		for i := 0; i < split; i++ {
			if i == 0 {
				sliceLen = left
			} else {
				sliceLen = k
			}
			gpidSlice := make([]*base.Gpid, sliceLen)
			for j := 0; j < sliceLen; j++ {
				gpidSlice[j] = allGpid[id]
				id++
			}
			scanners[i] = newPegasusScannerForUnorderedScanners(p, gpidSlice, options)
		}
		return scanners, nil
	}()
	return scanners, WrapError(err, OpGetUnorderedScanners)
}

func (p *pegasusTableConnector) CheckAndSet(ctx context.Context, hashKey []byte, checkSortKey []byte, checkType CheckType,
	checkOperand []byte, setSortKey []byte, setValue []byte, options *CheckAndSetOptions) (*CheckAndSetResult, error) {
	result, err := func() (*CheckAndSetResult, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, err
		}

		gpid, part := p.getPartition(hashKey)

		request := rrdb.NewCheckAndSetRequest()
		request.CheckType = rrdb.CasCheckType(checkType)
		request.CheckOperand = &base.Blob{Data: checkOperand}
		request.CheckSortKey = &base.Blob{Data: checkSortKey}
		request.HashKey = &base.Blob{Data: hashKey}
		request.SetExpireTsSeconds = 0
		if options.SetValueTTLSeconds != 0 {
			request.SetExpireTsSeconds = expireTsSeconds(time.Second * time.Duration(options.SetValueTTLSeconds))
		}
		request.SetSortKey = &base.Blob{Data: setSortKey}
		request.SetValue = &base.Blob{Data: setValue}
		request.ReturnCheckValue = options.ReturnCheckValue
		if !bytes.Equal(checkSortKey, setSortKey) {
			request.SetDiffSortKey = true
		} else {
			request.SetDiffSortKey = false
		}

		resp, err := part.CheckAndSet(ctx, gpid, request)

		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		if err == base.TryAgain {
			err = nil
		}
		if err = p.handleReplicaError(err, gpid, part); err != nil {
			return nil, err
		}

		result := &CheckAndSetResult{
			SetSucceed:         resp.Error == 0,
			CheckValueReturned: resp.CheckValueReturned,
			CheckValueExist:    resp.CheckValueReturned && resp.CheckValueExist,
		}
		if resp.CheckValueReturned && resp.CheckValueExist && resp.CheckValue != nil && resp.CheckValue.Data != nil && len(resp.CheckValue.Data) != 0 {
			result.CheckValue = resp.CheckValue.Data
		}
		return result, nil
	}()
	return result, WrapError(err, OpCheckAndSet)
}

func (p *pegasusTableConnector) SortKeyCount(ctx context.Context, hashKey []byte) (int64, error) {
	count, err := func() (int64, error) {
		if err := validateHashKey(hashKey); err != nil {
			return 0, err
		}

		gpid, part := p.getPartition(hashKey)
		resp, err := part.SortKeyCount(ctx, gpid, &base.Blob{Data: hashKey})
		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		if err = p.handleReplicaError(err, gpid, part); err != nil {
			return 0, err
		}
		return resp.Count, nil
	}()
	return count, WrapError(err, OpSortKeyCount)
}

func getPartitionIndex(hashKey []byte, partitionCount int) int32 {
	return int32(crc64Hash(hashKey) % uint64(partitionCount))
}

func (p *pegasusTableConnector) getPartition(hashKey []byte) (*base.Gpid, *session.ReplicaSession) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	gpid := &base.Gpid{
		Appid:          p.appID,
		PartitionIndex: getPartitionIndex(hashKey, len(p.parts)),
	}
	part := p.parts[gpid.PartitionIndex].session

	return gpid, part
}

func (p *pegasusTableConnector) getPartitionByGpid(gpid *base.Gpid) *session.ReplicaSession {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.parts[gpid.PartitionIndex].session
}

func (p *pegasusTableConnector) Close() error {
	p.tom.Kill(errors.New("table closed"))
	return nil
}

func (p *pegasusTableConnector) handleReplicaError(err error, gpid *base.Gpid, replica *session.ReplicaSession) error {
	if err != nil {
		confUpdate := false

		switch err {
		case base.ERR_TIMEOUT:
		case context.DeadlineExceeded:
			// timeout will not trigger a configuration update

		case base.ERR_NOT_ENOUGH_MEMBER:
		case base.ERR_CAPACITY_EXCEEDED:

		default:
			confUpdate = true
		}

		if confUpdate {
			// we need to check if there's newer configuration.
			p.tryConfUpdate()
		}

		// add gpid and remote address to error
		perr := WrapError(err, 0).(*PError)
		if perr.Err != nil {
			perr.Err = fmt.Errorf("%s [%s, %s]", perr.Err, gpid, replica)
		} else {
			perr.Err = fmt.Errorf("[%s, %s]", gpid, replica)
		}
		return perr
	}
	return nil
}

/// Don't bother if there's ongoing attempt.
func (p *pegasusTableConnector) tryConfUpdate() {
	select {
	case p.confUpdateCh <- true:
	default:
	}
}

func (p *pegasusTableConnector) loopForAutoUpdate() error {
	for {
		select {
		case <-p.confUpdateCh:
			p.selfUpdate()
		case <-p.tom.Dying():
			return nil
		}

		// sleep a while
		select {
		case <-time.After(time.Second):
		case <-p.tom.Dying():
			return nil
		}
	}
}

func (p *pegasusTableConnector) selfUpdate() bool {
	// ignore the returned error
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	if err := p.updateConf(ctx); err != nil {
		p.logger.Printf("self update failed [table: %s]: %s", p.tableName, err.Error())
	}

	// flush confUpdateCh
	select {
	case <-p.confUpdateCh:
	default:
	}

	return true
}

func (p *pegasusTableConnector) getGpid(key []byte) (*base.Gpid, error) {
	if key == nil || len(key) < 2 {
		return nil, fmt.Errorf("unable to getGpid by key: %s", key)
	}

	hashKeyLen := 0xFFFF & binary.BigEndian.Uint16(key[:2])
	if hashKeyLen != 0xFFFF && int(2+hashKeyLen) <= len(key) {
		gpid := &base.Gpid{Appid: p.appID}
		if hashKeyLen == 0 {
			gpid.PartitionIndex = int32(crc64Hash(key[2:]) % uint64(len(p.parts)))
		} else {
			gpid.PartitionIndex = int32(crc64Hash(key[2:hashKeyLen+2]) % uint64(len(p.parts)))
		}
		return gpid, nil

	}
	return nil, fmt.Errorf("unable to getGpid, hashKey length invalid")
}

func (p *pegasusTableConnector) getAllGpid() []*base.Gpid {
	p.mu.RLock()
	defer p.mu.RUnlock()
	count := len(p.parts)
	ret := make([]*base.Gpid, count)
	for i := 0; i < count; i++ {
		ret[i] = &base.Gpid{Appid: p.appID, PartitionIndex: int32(i)}
	}
	return ret
}
