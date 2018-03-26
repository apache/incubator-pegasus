// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/session"
	"gopkg.in/tomb.v2"
)

type TableConnector interface {
	Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error)

	Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error

	Del(ctx context.Context, hashKey []byte, sortKey []byte) error

	MultiGet(ctx context.Context, hashKey []byte, sortKeys [][]byte, options MultiGetOptions) ([]KeyValue, error)

	MultiGetRange(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options MultiGetOptions) ([]KeyValue, error)

	MultiDel(ctx context.Context, hashKey []byte, sortKeys [][]byte) error

	Close() error
}

type pegasusTableConnector struct {
	meta    *session.MetaManager
	replica *session.ReplicaManager

	tableName string
	appId     int32
	parts     []*replicaNode
	mu        sync.RWMutex

	confUpdateCh       chan bool
	lastConfUpdateTime time.Time
	tom                tomb.Tomb
}

type replicaNode struct {
	session *session.ReplicaSession
	pconf   *replication.PartitionConfiguration
}

// Query for the configuration of the given table, and set up connection to
// the replicas which the table locates on.
func connectTable(ctx context.Context, tableName string, meta *session.MetaManager, replica *session.ReplicaManager) (TableConnector, error) {
	p := &pegasusTableConnector{
		tableName:    tableName,
		meta:         meta,
		replica:      replica,
		confUpdateCh: make(chan bool, 1),
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
	defer func() {
		p.lastConfUpdateTime = time.Now()
	}()
	if err != nil {
		return fmt.Errorf("connect table(%s): %s", p.tableName, err)
	}
	if resp.Err.Errno != base.ERR_OK.String() {
		// TODO(wutao1): convert Errno(type string) to base.ErrType
		return fmt.Errorf("connect table(%s): %s", p.tableName, resp.Err.Errno)
	}

	p.mu.Lock()
	p.appId = resp.AppID
	p.parts = make([]*replicaNode, len(resp.Partitions))
	// TODO(wutao1): make sure PartitionIndex are continuous
	for _, pconf := range resp.Partitions {
		r := &replicaNode{
			pconf:   pconf,
			session: p.replica.GetReplica(pconf.Primary.GetAddress()),
		}
		p.parts[pconf.Pid.PartitionIndex] = r
	}
	p.mu.Unlock()

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
func wrapError(err error, op OpType) error {
	if err != nil {
		if pe, ok := err.(*PError); ok {
			pe.Op = op
			return pe
		} else if be, ok := err.(base.ErrType); ok {
			return &PError{
				Err:  nil,
				Op:   op,
				Code: be,
			}
		}
		return &PError{
			Err:  err,
			Op:   op,
			Code: base.ERR_CLIENT_FAILED,
		}
	}
	return nil
}

func (p *pegasusTableConnector) Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error) {
	bytes, err := func() ([]byte, error) {
		if err := validateHashKey(hashKey); err != nil {
			return nil, err
		}

		key := encodeHashKeySortKey(hashKey, sortKey)
		gpid, part := p.getPartition(hashKey)

		resp, err := part.Get(ctx, gpid, key)
		if err = p.handleError(err, gpid, part); err != nil {
			return nil, err
		} else {
			return resp.Value.Data, nil
		}
	}()
	return bytes, wrapError(err, OpGet)
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
			err = base.NewDsnErrFromInt(resp.Error)
		}
		return p.handleError(err, gpid, part)
	}()
	return wrapError(err, OpSet)
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
			err = base.NewDsnErrFromInt(resp.Error)
		}
		return p.handleError(err, gpid, part)
	}()
	return wrapError(err, OpDel)
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
	return kvs, wrapError(err, OpMultiGet)
}

func (p *pegasusTableConnector) MultiGetRange(ctx context.Context, hashKey []byte, startSortKey []byte, stopSortKey []byte, options MultiGetOptions) ([]KeyValue, error) {
	request := rrdb.NewMultiGetRequest()
	request.HashKey = &base.Blob{Data: hashKey}
	request.StartSortkey = &base.Blob{Data: startSortKey}
	request.StopSortkey = &base.Blob{Data: stopSortKey}
	setRequestByOption(options, request)

	kvs, err := p.doMultiGet(ctx, hashKey, request)
	return kvs, wrapError(err, OpMultiGetRange)
}

func (p *pegasusTableConnector) doMultiGet(ctx context.Context, hashKey []byte, request *rrdb.MultiGetRequest) ([]KeyValue, error) {
	if err := validateHashKey(hashKey); err != nil {
		return nil, err
	}

	gpid, part := p.getPartition(hashKey)
	resp, err := part.MultiGet(ctx, gpid, request)

	if err == nil {
		err = base.NewDsnErrFromInt(resp.Error)
	}
	if err = p.handleError(err, gpid, part); err == nil {
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
			err = base.NewDsnErrFromInt(resp.Error)
		}
		return p.handleError(err, gpid, part)
	}()
	return wrapError(err, OpMultiDel)
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

func (p *pegasusTableConnector) Close() error {
	p.tom.Kill(errors.New("table closed"))
	err := func() error {
		err := p.meta.Close()
		if err == nil {
			for _, r := range p.parts {
				<-r.session.Close()
			}
		}
		return err
	}()
	return wrapError(err, OpClose)
}

func (p *pegasusTableConnector) handleError(err error, gpid *base.Gpid, replica *session.ReplicaSession) error {
	if err != nil {
		// when err != nil, it means the network connection between client and replicas
		// may be illed, hence we need to check if there's newer configuration.

		// trigger configuration update
		p.tryConfUpdate()

		// add gpid and remote address to error
		perr := wrapError(err, 0).(*PError)
		perr.Err = fmt.Errorf("%s [%s, %s]", perr.Err, gpid, replica)
		return perr
	}
	return nil
}

func (p *pegasusTableConnector) tryConfUpdate() {
	select {
	case p.confUpdateCh <- true:
	default:
	}
}

// For each table there's a background worker pulling down the latest
// version of configuration from meta server automatically.
// The configuration update strategy can be stated as follow:
//
//  - When a table operation encountered error, it will trigger a
//    new round of self update if there's no one in progress.
//  - The interval of two subsequent config updates should be larger
//	  than 5 sec.
//
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
	if time.Now().Sub(p.lastConfUpdateTime) < 5*time.Second {
		return false
	}

	// ignore the returned error
	ctx, _ := context.WithTimeout(context.Background(), time.Second*2)
	p.updateConf(ctx)

	// flush confUpdateCh
	select {
	case <-p.confUpdateCh:
	default:
	}

	return true
}
