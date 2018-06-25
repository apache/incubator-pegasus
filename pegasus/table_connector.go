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

type TableConnector interface {
	Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error)

	Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error

	Del(ctx context.Context, hashKey []byte, sortKey []byte) error

	MultiGet(ctx context.Context, hashKey []byte, sortKeys [][]byte, options MultiGetOptions) ([]KeyValue, error)

	MultiGetRange(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options MultiGetOptions) ([]KeyValue, error)

	MultiSet(ctx context.Context, hashKey []byte, sortKeys [][]byte, values [][]byte, ttlSeconds int) error

	MultiDel(ctx context.Context, hashKey []byte, sortKeys [][]byte) error

	TTL(ctx context.Context, hashKey []byte, sortKey []byte) (int, error)

	Exist(ctx context.Context, hashKey []byte, sortKey []byte) (bool, error)

	GetScanner(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options *ScannerOptions) (Scanner, error)

	GetUnorderedScanners(ctx context.Context, maxSplitCount int, options *ScannerOptions) ([]Scanner, error)

	Close() error
}

type pegasusTableConnector struct {
	meta    *session.MetaManager
	replica *session.ReplicaManager

	logger pegalog.Logger

	tableName string
	appId     int32
	parts     []*replicaNode
	mu        sync.RWMutex

	confUpdateCh chan bool
	tom          tomb.Tomb
}

type replicaNode struct {
	session *session.ReplicaSession
	pconf   *replication.PartitionConfiguration
}

// Query for the configuration of the given table, and set up connection to
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

	p.appId = resp.AppID

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
			return fmt.Errorf("unable to resolve routing table [appid: %d]: [%v]", p.appId, pconf)
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
	if len(hashKey) == 0 || hashKey == nil {
		return fmt.Errorf("InvalidParameter: hash key must not be empty")
	}
	if len(hashKey) > math.MaxUint16 {
		return fmt.Errorf("InvalidParameter: length of hash key (%d) must be less than %d", len(hashKey), math.MaxUint16)
	}
	return nil
}

// Wraps up the internal errors for ensuring that all types of errors
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
		} else {
			return resp.Value.Data, nil
		}
	}()
	return b, WrapError(err, OpGet)
}

func (p *pegasusTableConnector) Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error {
	err := func() error {
		if err := validateHashKey(hashKey); err != nil {
			return err
		}

		key := encodeHashKeySortKey(hashKey, sortKey)
		val := &base.Blob{Data: value}
		gpid, part := p.getPartition(hashKey)

		resp, err := part.Put(ctx, gpid, key, val)
		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		return p.handleReplicaError(err, gpid, part)
	}()
	return WrapError(err, OpSet)
}

func (p *pegasusTableConnector) Del(ctx context.Context, hashKey []byte, sortKey []byte) error {
	err := func() error {
		if err := validateHashKey(hashKey); err != nil {
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

func setRequestByOption(options MultiGetOptions, request *rrdb.MultiGetRequest) {
	request.MaxKvCount = int32(options.MaxFetchCount)
	request.MaxKvSize = int32(options.MaxFetchSize)
	request.StartInclusive = options.StartInclusive
	request.StopInclusive = options.StopInclusive
	request.SortKeyFilterType = rrdb.FilterType(options.SortKeyFilter.Type)
	request.SortKeyFilterPattern = &base.Blob{Data: options.SortKeyFilter.Pattern}
}

func (p *pegasusTableConnector) MultiGet(ctx context.Context, hashKey []byte, sortKeys [][]byte, options MultiGetOptions) ([]KeyValue, error) {
	request := rrdb.NewMultiGetRequest()
	request.HashKey = &base.Blob{Data: hashKey}
	request.SorkKeys = make([]*base.Blob, len(sortKeys))
	request.StartSortkey = &base.Blob{}
	request.StopSortkey = &base.Blob{}
	for i, sortKey := range sortKeys {
		request.SorkKeys[i] = &base.Blob{Data: sortKey}
	}
	setRequestByOption(options, request)

	kvs, err := p.doMultiGet(ctx, hashKey, request)
	return kvs, WrapError(err, OpMultiGet)
}

func (p *pegasusTableConnector) MultiGetRange(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options MultiGetOptions) ([]KeyValue, error) {
	request := rrdb.NewMultiGetRequest()
	request.HashKey = &base.Blob{Data: hashKey}
	request.StartSortkey = &base.Blob{Data: startSortKey}
	request.StopSortkey = &base.Blob{Data: stopSortKey}
	setRequestByOption(options, request)

	kvs, err := p.doMultiGet(ctx, hashKey, request)
	return kvs, WrapError(err, OpMultiGetRange)
}

func (p *pegasusTableConnector) doMultiGet(ctx context.Context, hashKey []byte, request *rrdb.MultiGetRequest) ([]KeyValue, error) {
	if err := validateHashKey(hashKey); err != nil {
		return nil, err
	}

	gpid, part := p.getPartition(hashKey)
	resp, err := part.MultiGet(ctx, gpid, request)

	if err == nil {
		err = base.NewRocksDBErrFromInt(resp.Error)
	}
	if err = p.handleReplicaError(err, gpid, part); err == nil {
		kvs := make([]KeyValue, len(resp.Kvs))
		for i, blobKv := range resp.Kvs {
			kvs[i].SortKey = blobKv.Key.Data
			kvs[i].Value = blobKv.Value.Data
		}

		sort.Slice(kvs, func(i, j int) bool {
			return bytes.Compare(kvs[i].SortKey, kvs[j].SortKey) < 0
		})

		return kvs, nil
	}
	return nil, err
}

func (p *pegasusTableConnector) MultiSet(ctx context.Context, hashKey []byte, sortKeys [][]byte, values [][]byte, ttlSeconds int) error {
	request := rrdb.NewMultiPutRequest()
	request.HashKey = &base.Blob{Data: hashKey}
	request.Kvs = make([]*rrdb.KeyValue, len(sortKeys))

	for i := 0; i < len(sortKeys); i++ {
		request.Kvs[i] = &rrdb.KeyValue{
			Key:   &base.Blob{Data: sortKeys[i]},
			Value: &base.Blob{Data: values[i]},
		}
	}

	if ttlSeconds == 0 {
		request.ExpireTsSeconds = 0
	} else {
		// 1451606400 means 2016.01.01-00:00:00 GMT
		request.ExpireTsSeconds = int32(int64(ttlSeconds) + time.Now().Unix() - 1451606400)
	}

	err := p.doMultiSet(ctx, hashKey, request)
	return WrapError(err, OpMultiSet)
}

func (p *pegasusTableConnector) doMultiSet(ctx context.Context, hashKey []byte, request *rrdb.MultiPutRequest) error {
	if err := validateHashKey(hashKey); err != nil {
		return err
	}

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

		gpid, part := p.getPartition(hashKey)

		request := rrdb.NewMultiRemoveRequest()
		request.HashKey = &base.Blob{Data: hashKey}
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

func (p *pegasusTableConnector) TTL(ctx context.Context, hashKey []byte, sortKey []byte) (int, error) {
	ttl, err := func() (int, error) {
		if err := validateHashKey(hashKey); err != nil {
			return 0, err
		}

		key := encodeHashKeySortKey(hashKey, sortKey)
		gpid, part := p.getPartition(hashKey)

		resp, err := part.TTL(ctx, gpid, key)
		if err == nil {
			err = base.NewRocksDBErrFromInt(resp.Error)
		}
		if err = p.handleReplicaError(err, gpid, part); err != nil {
			return -2, err
		} else {
			return int(resp.GetTTLSeconds()), nil
		}
	}()
	return ttl, WrapError(err, OpTTL)
}

func (p *pegasusTableConnector) Exist(ctx context.Context, hashKey []byte, sortKey []byte) (bool, error) {
	ttl, err := p.TTL(ctx, hashKey, sortKey)

	if err == nil {
		if ttl == -2 {
			return false, nil
		} else {
			return true, nil
		}
	}
	return false, WrapError(err, OpTTL)
}

func (p *pegasusTableConnector) GetScanner(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte,
	options *ScannerOptions) (Scanner, error) {
	scanner, err := func() (Scanner, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, err
		}

		start := encodeHashKeySortKey(hashKey, startSortKey)
		var stop *base.Blob
		if stopSortKey == nil || len(stopSortKey) == 0 {
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

func getPartitionIndex(hashKey []byte, partitionCount int) int32 {
	return int32(crc64Hash(hashKey) % uint64(partitionCount))
}

func (p *pegasusTableConnector) getPartition(hashKey []byte) (*base.Gpid, *session.ReplicaSession) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	gpid := &base.Gpid{
		Appid:          p.appId,
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
	return nil
}

func (p *pegasusTableConnector) selfUpdate() bool {
	// ignore the returned error
	ctx, _ := context.WithTimeout(context.Background(), time.Second*10)
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

func restoreSortKeyHashKey(key []byte) (error, []byte, []byte) {
	if key == nil || len(key) < 2 {
		return fmt.Errorf("unable to restore key: %s", key), nil, nil
	}

	hashKeyLen := 0xFFFF & binary.BigEndian.Uint16(key[:2])
	if hashKeyLen != 0xFFFF && int(2+hashKeyLen) <= len(key) {
		hashKey := key[2 : 2+hashKeyLen]
		sortKey := key[2+hashKeyLen:]
		return nil, hashKey, sortKey
	}

	return fmt.Errorf("unable to restore key, hashKey length invalid"), nil, nil
}

func (p *pegasusTableConnector) getGpid(key []byte) (*base.Gpid, error) {
	if key == nil || len(key) < 2 {
		return nil, fmt.Errorf("unable to getGpid by key: %s", key)
	}

	hashKeyLen := 0xFFFF & binary.BigEndian.Uint16(key[:2])
	if hashKeyLen != 0xFFFF && int(2+hashKeyLen) <= len(key) {
		gpid := &base.Gpid{Appid: p.appId}
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
		ret[i] = &base.Gpid{Appid: p.appId, PartitionIndex: int32(i)}
	}
	return ret
}
