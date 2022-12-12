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

package pegasus

import (
	"context"
	"encoding/binary"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/apache/incubator-pegasus/go-client/idl/rrdb"
	"github.com/apache/incubator-pegasus/go-client/pegalog"
)

// ScannerOptions is the options for GetScanner and GetUnorderedScanners.
type ScannerOptions struct {
	BatchSize      int  // internal buffer batch size
	StartInclusive bool // if the startSortKey is included
	StopInclusive  bool // if the stopSortKey is included
	HashKeyFilter  Filter
	SortKeyFilter  Filter
	NoValue        bool // only fetch hash_key and sort_key, but not fetch value
}

const (
	batchScanning     = 0
	batchScanFinished = -1 // Scanner's batch is finished, clean up it and switch to the status batchEmpty
	batchEmpty        = -2 // scan context has been removed
	batchRpcError     = -3 // rpc error, include ERR_SESSION_RESET,ERR_OBJECT_NOT_FOUND,ERR_INVALID_STATE, ERR_TIMEOUT
	batchUnknownError = -4 // rpc succeed, but operation encounter some unknown error in server side
)

// Scanner defines the interface of client-side scanning.
type Scanner interface {
	// Grabs the next entry.
	Next(ctx context.Context) (completed bool, hashKey []byte, sortKey []byte, value []byte, err error)

	Close() error
}

type pegasusScanner struct {
	table    *pegasusTableConnector
	startKey *base.Blob
	stopKey  *base.Blob
	options  *ScannerOptions

	gpidSlice []*base.Gpid
	hashSlice []uint64
	gpidIndex int        // index of gpidSlice[] and hashSlice[]
	curGpid   *base.Gpid // current gpid
	curHash   uint64     // current partitionHash
	checkHash bool

	batchEntries []*KeyValue
	batchIndex   int
	batchStatus  int64

	isNextRunning atomic.Value

	closed bool
	logger pegalog.Logger
}

// NewScanOptions returns the default ScannerOptions.
func NewScanOptions() *ScannerOptions {
	return &ScannerOptions{
		BatchSize:      1000,
		StartInclusive: true,
		StopInclusive:  false,
		HashKeyFilter:  Filter{Type: FilterTypeNoFilter, Pattern: nil},
		SortKeyFilter:  Filter{Type: FilterTypeNoFilter, Pattern: nil},
		NoValue:        false,
	}
}

func newPegasusScannerImpl(table *pegasusTableConnector, gpidSlice []*base.Gpid, hashSlice []uint64, options *ScannerOptions,
	startKey *base.Blob, stopKey *base.Blob, checkHash bool) Scanner {
	scanner := &pegasusScanner{
		table:        table,
		gpidSlice:    gpidSlice,
		hashSlice:    hashSlice,
		checkHash:    checkHash,
		options:      options,
		startKey:     startKey,
		stopKey:      stopKey,
		batchIndex:   -1,
		batchStatus:  batchScanFinished,
		gpidIndex:    len(gpidSlice),
		batchEntries: make([]*KeyValue, 0),
		closed:       false,
		logger:       pegalog.GetLogger(),
	}
	scanner.isNextRunning.Store(0)
	return scanner
}

func newPegasusScanner(table *pegasusTableConnector, gpid *base.Gpid, partitionHash uint64, options *ScannerOptions,
	startKey *base.Blob, stopKey *base.Blob) Scanner {
	gpidSlice := []*base.Gpid{gpid}
	hashSlice := []uint64{partitionHash}
	return newPegasusScannerImpl(table, gpidSlice, hashSlice, options, startKey, stopKey, false)
}

func newPegasusScannerForUnorderedScanners(table *pegasusTableConnector, gpidSlice []*base.Gpid, hashSlice []uint64,
	options *ScannerOptions) Scanner {
	options.StartInclusive = true
	options.StopInclusive = false
	return newPegasusScannerImpl(table, gpidSlice, hashSlice, options, &base.Blob{Data: []byte{0x00, 0x00}},
		&base.Blob{Data: []byte{0xFF, 0xFF}}, true)
}

func (p *pegasusScanner) Next(ctx context.Context) (completed bool, hashKey []byte,
	sortKey []byte, value []byte, err error) {
	if p.batchStatus == batchUnknownError {
		err = fmt.Errorf("last Next() encounter unknow error, please retry after resloving it manually")
		return
	}
	if p.batchStatus == batchRpcError {
		err = fmt.Errorf("last Next() encounter rpc error, it may recover after next loop")
		p.batchStatus = batchScanning
		return
	}
	if p.closed {
		err = fmt.Errorf("scanner is closed")
		return
	}

	completed, hashKey, sortKey, value, err = func() (completed bool, hashKey []byte, sortKey []byte, value []byte, err error) {
		// TODO(tangyanzhao): This method is not thread safe, should use r/w lock
		// Prevent two concurrent calls on Next of the same Scanner.
		if p.isNextRunning.Load() != 0 {
			err = fmt.Errorf("there can be no concurrent calls on Next of the same Scanner")
			return
		}
		p.isNextRunning.Store(1)
		defer p.isNextRunning.Store(0)
		return p.doNext(ctx)
	}()

	err = WrapError(err, OpNext)
	return
}

func (p *pegasusScanner) doNext(ctx context.Context) (completed bool, hashKey []byte,
	sortKey []byte, value []byte, err error) {
	// until we have the valid batch
	for p.batchIndex++; p.batchIndex >= len(p.batchEntries); p.batchIndex++ {
		if p.batchStatus == batchScanFinished {
			if p.gpidIndex <= 0 {
				completed = true
				p.logger.Print(" Scanning on all partitions has been completed")
				return
			}
			p.gpidIndex--
			p.curGpid = p.gpidSlice[p.gpidIndex]
			p.curHash = p.hashSlice[p.gpidIndex]
			p.batchClear()
		} else if p.batchStatus == batchEmpty {
			return p.startScanPartition(ctx)
		} else {
			// request nextBatch
			return p.nextBatch(ctx)
		}
	}
	// batch.SortKey=<hashKey,sortKey>
	hashKey, sortKey, err = restoreSortKeyHashKey(p.batchEntries[p.batchIndex].SortKey)
	value = p.batchEntries[p.batchIndex].Value
	return
}

func (p *pegasusScanner) batchClear() {
	p.batchEntries = make([]*KeyValue, 0)
	p.batchIndex = -1
	p.batchStatus = batchEmpty
}

func (p *pegasusScanner) startScanPartition(ctx context.Context) (completed bool, hashKey []byte,
	sortKey []byte, value []byte, err error) {
	request := rrdb.NewGetScannerRequest()
	if len(p.batchEntries) == 0 {
		request.StartKey = p.startKey
		request.StartInclusive = p.options.StartInclusive
	} else {
		request.StartKey = &base.Blob{Data: p.batchEntries[len(p.batchEntries)-1].SortKey}
		request.StartInclusive = false
	}
	request.StopKey = p.stopKey
	request.StopInclusive = p.options.StopInclusive
	request.BatchSize = int32(p.options.BatchSize)
	request.NoValue = p.options.NoValue

	request.HashKeyFilterType = rrdb.FilterType(p.options.HashKeyFilter.Type)
	request.HashKeyFilterPattern = &base.Blob{}
	if p.options.HashKeyFilter.Pattern != nil {
		request.HashKeyFilterPattern.Data = p.options.HashKeyFilter.Pattern
	}

	request.SortKeyFilterType = rrdb.FilterType(p.options.SortKeyFilter.Type)
	request.SortKeyFilterPattern = &base.Blob{}
	if p.options.SortKeyFilter.Pattern != nil {
		request.SortKeyFilterPattern.Data = p.options.SortKeyFilter.Pattern
	}
	request.ValidatePartitionHash = &p.checkHash

	part := p.table.getPartitionByGpid(p.curGpid)
	response, err := part.GetScanner(ctx, p.curGpid, p.curHash, request)
	if err != nil {
		p.batchStatus = batchRpcError
		if updateConfig, _, errHandler := p.table.handleReplicaError(err, part); errHandler != nil {
			err = fmt.Errorf("scan failed, error = %s, try resolve it(updateConfig=%v), result = %s", err, updateConfig, errHandler)
		}
		return
	}
	err = p.onRecvScanResponse(response, err)
	if err == nil {
		return p.doNext(ctx)
	}

	return
}

func (p *pegasusScanner) nextBatch(ctx context.Context) (completed bool, hashKey []byte,
	sortKey []byte, value []byte, err error) {
	request := &rrdb.ScanRequest{ContextID: p.batchStatus}
	part := p.table.getPartitionByGpid(p.curGpid)
	response, err := part.Scan(ctx, p.curGpid, p.curHash, request)
	if err != nil {
		p.batchStatus = batchRpcError
		if updateConfig, _, errHandler := p.table.handleReplicaError(err, part); errHandler != nil {
			err = fmt.Errorf("scan failed, error = %s, try resolve it(updateConfig=%v), result = %s", err, updateConfig, errHandler)
		}
		return
	}
	err = p.onRecvScanResponse(response, err)
	if err == nil {
		return p.doNext(ctx)
	}
	return
}

func (p *pegasusScanner) onRecvScanResponse(response *rrdb.ScanResponse, err error) error {
	if err == nil {
		if response.Error == 0 {
			// ERR_OK
			p.batchEntries = make([]*KeyValue, len(response.Kvs))
			for i := 0; i < len(response.Kvs); i++ {
				p.batchEntries[i] = &KeyValue{
					SortKey: response.Kvs[i].Key.Data, // batch.SortKey=<hashKey,sortKey>
					Value:   response.Kvs[i].Value.Data,
				}
			}

			p.batchIndex = -1
			p.batchStatus = response.ContextID
		} else if response.Error == 1 {
			// scan context has been removed
			p.batchStatus = batchEmpty
		} else {
			// rpc succeed, but operation encounter some error in server side
			p.batchStatus = batchUnknownError
			return base.NewRocksDBErrFromInt(response.Error)
		}
		return nil
	}

	return fmt.Errorf("scan failed with error:" + err.Error())
}

func (p *pegasusScanner) Close() error {
	var err error

	// try to close in 100ms,
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	// if batchScanFinished or batchEmpty, server side will clear scanner automatically
	// if not, clear scanner manually
	if p.batchStatus >= batchScanning {
		part := p.table.getPartitionByGpid(p.curGpid)
		err = part.ClearScanner(ctx, p.curGpid, p.curHash, p.batchStatus)
		if err == nil {
			p.batchStatus = batchScanFinished
		}
	}

	p.gpidIndex = 0
	p.closed = true
	return WrapError(err, OpScannerClose)
}

func restoreSortKeyHashKey(key []byte) (hashKey []byte, sortKey []byte, err error) {
	if key == nil || len(key) < 2 {
		return nil, nil, fmt.Errorf("unable to restore key: %s", key)
	}

	hashKeyLen := 0xFFFF & binary.BigEndian.Uint16(key[:2])
	if hashKeyLen != 0xFFFF && int(2+hashKeyLen) <= len(key) {
		hashKey = key[2 : 2+hashKeyLen]
		sortKey = key[2+hashKeyLen:]
		return hashKey, sortKey, nil
	}

	return nil, nil, fmt.Errorf("unable to restore key, hashKey length invalid")
}
