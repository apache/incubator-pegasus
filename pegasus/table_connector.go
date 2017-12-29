// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/pegasus-kv/pegasus-go-client/idl/base"
	"github.com/pegasus-kv/pegasus-go-client/idl/replication"
	"github.com/pegasus-kv/pegasus-go-client/session"
	"gopkg.in/tomb.v2"
)

type TableConnector interface {
	Get(ctx context.Context, hashKey []byte, sortKey []byte) ([]byte, error)

	Set(ctx context.Context, hashKey []byte, sortKey []byte, value []byte) error

	Del(ctx context.Context, hashKey []byte, sortKey []byte) error

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

	// p.tom.Go(p.loopForAutoUpdate)
	return p, nil
}

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

func (p *pegasusTableConnector) getGpid(hashKey []byte) *base.Gpid {
	return &base.Gpid{
		Appid:          p.appId,
		PartitionIndex: int32(crc64Hash(hashKey) % uint64(len(p.parts))),
	}
}

func validateHashKey(hashKey []byte) error {
	if len(hashKey) > math.MaxUint16 {
		return fmt.Errorf("length of hash key (%d) must be less than %d", len(hashKey), math.MaxUint16)
	}
	return nil
}

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
		if err != nil {
			return nil, p.handleError(err)
		} else {
			if err = p.handleErrorCode(base.ErrType(resp.Error)); err != nil {
				return nil, err
			} else {
				return resp.Value.Data, nil
			}
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
		if err != nil {
			return p.handleError(err)
		} else {
			return p.handleErrorCode(base.ErrType(resp.Error))
		}
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
		if err != nil {
			return p.handleError(err)
		} else {
			return p.handleErrorCode(base.ErrType(resp.Error))
		}
	}()
	return wrapError(err, OpDel)
}

func (p *pegasusTableConnector) getPartition(hashKey []byte) (*base.Gpid, *session.ReplicaSession) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	gpid := &base.Gpid{
		Appid:          p.appId,
		PartitionIndex: int32(crc64Hash(hashKey) % uint64(len(p.parts))),
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
				if err = r.session.Close(); err != nil {
					return err
				}
			}
		}
		return err
	}()
	return wrapError(err, OpClose)
}

// TODO(wutao1): make gpid included in the returned error.
func (p *pegasusTableConnector) handleErrorCode(respErr base.ErrType) error {
	return p.doHandleError(respErr, nil)
}

func (p *pegasusTableConnector) handleError(err error) error {
	return p.doHandleError(base.ERR_OK, err)
}

func (p *pegasusTableConnector) doHandleError(respErr base.ErrType, err error) error {
	if err != nil ||
		respErr == base.ERR_OBJECT_NOT_FOUND ||
		respErr == base.ERR_INVALID_STATE {

		// when err != nil, it means the network connection between client and replicas
		// may be illed, hence we need to check if there's newer configuration.
		// when respErr == ERR_OBJECT_NOT_FOUND, it means the replica server doesn't
		// serve this gpid.
		// when respErr == ERR_INVALID_STATE, it indicates that replica server is not
		// primary.

		// trigger configuration update
		select {
		case p.confUpdateCh <- true:
		default:
		}
	}

	if err != nil {
		return err
	}
	if respErr != base.ERR_OK {
		return respErr
	}
	return nil
}

// For each table there's a background worker pulling down the latest
// version of configuration from meta server automatically.
// The configuration update strategy can be stated as follow:
//
// 	- Every [5, 10) seconds it will query meta for configuration.
//  - When a table operation encountered error, it will trigger a
//	  new round of self update if there's no one in progress.
//  -
//
func (p *pegasusTableConnector) loopForAutoUpdate() error {
	for {
		select {
		case <-p.confUpdateCh:
			p.selfUpdate()
		case <-time.After(time.Second * time.Duration(5+rand.Intn(5))):
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.updateConf(ctx)

	// flush confUpdateCh
	select {
	case <-p.confUpdateCh:
	default:
	}

	return true
}
