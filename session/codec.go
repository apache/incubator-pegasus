// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package session

import (
	"bytes"
	"fmt"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
	"github.com/XiaoMi/pegasus-go-client/idl/replication"
	"github.com/XiaoMi/pegasus-go-client/idl/rrdb"
	"github.com/XiaoMi/pegasus-go-client/pegalog"
	"github.com/apache/thrift/lib/go/thrift"
)

type PegasusCodec struct {
	logger pegalog.Logger
}

func (p *PegasusCodec) Marshal(v interface{}) ([]byte, error) {
	r, _ := v.(*rpcCall)

	header := &thriftHeader{
		headerLength:   uint32(thriftHeaderBytesLen),
		appId:          r.gpid.Appid,
		partitionIndex: r.gpid.PartitionIndex,
		threadHash:     gpidToThreadHash(r.gpid),
		partitionHash:  0,
	}

	// skip the first ThriftHeaderBytesLen bytes
	buf := thrift.NewTMemoryBuffer()
	buf.Write(make([]byte, thriftHeaderBytesLen))

	// encode body into buffer
	oprot := thrift.NewTBinaryProtocolTransport(buf)

	var err error
	if err = oprot.WriteMessageBegin(r.name, thrift.CALL, r.seqId); err != nil {
		return nil, err
	}
	if err = r.args.Write(oprot); err != nil {
		return nil, err
	}
	if err = oprot.WriteMessageEnd(); err != nil {
		return nil, err
	}

	// encode header into buffer
	header.bodyLength = uint32(buf.Len() - thriftHeaderBytesLen)
	header.marshall(buf.Bytes()[0:thriftHeaderBytesLen])

	return buf.Bytes(), nil
}

func (p *PegasusCodec) Unmarshal(data []byte, v interface{}) error {
	r, _ := v.(*rpcCall)

	iprot := thrift.NewTBinaryProtocolTransport(thrift.NewStreamTransportR(bytes.NewBuffer(data)))
	ec := &base.ErrorCode{}
	if err := ec.Read(iprot); err != nil {
		return err
	}

	name, _, seqId, err := iprot.ReadMessageBegin()
	if err != nil {
		return err
	}

	r.name = name
	r.seqId = seqId

	if ec.Errno != base.ERR_OK.String() {
		// convert string to base.ErrType
		err, parseErr := base.ErrTypeString(ec.Errno)
		if parseErr != nil {
			p.logger.Println("failed to unmarshal the heading error code of rpc response: ", parseErr)
			return parseErr
		}

		r.err = err
		return nil
	}

	nameToResultFunc, ok := nameToResultMap[name]
	if !ok {
		return fmt.Errorf("failed to find rpc name: %s", name)
	}
	r.result = nameToResultFunc()

	// read response body
	if err = r.result.Read(iprot); err != nil {
		return err
	}
	if err = iprot.ReadMessageEnd(); err != nil {
		return err
	}

	return nil
}

func (p *PegasusCodec) String() string {
	return "pegasus"
}

var nameToResultMap = map[string]func() rpcResponseResult{
	"RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX_ACK": func() rpcResponseResult {
		return &rrdb.MetaQueryCfgResult{
			Success: replication.NewQueryCfgResponse(),
		}
	},
	"RPC_RRDB_RRDB_GET_ACK": func() rpcResponseResult {
		return &rrdb.RrdbGetResult{
			Success: rrdb.NewReadResponse(),
		}
	},
	"RPC_RRDB_RRDB_PUT_ACK": func() rpcResponseResult {
		return &rrdb.RrdbPutResult{
			Success: rrdb.NewUpdateResponse(),
		}
	},
	"RPC_RRDB_RRDB_REMOVE_ACK": func() rpcResponseResult {
		return &rrdb.RrdbRemoveResult{
			Success: rrdb.NewUpdateResponse(),
		}
	},
	"RPC_RRDB_RRDB_MULTI_GET_ACK": func() rpcResponseResult {
		return &rrdb.RrdbMultiGetResult{
			Success: rrdb.NewMultiGetResponse(),
		}
	},
	"RPC_RRDB_RRDB_MULTI_REMOVE_ACK": func() rpcResponseResult {
		return &rrdb.RrdbMultiRemoveResult{
			Success: rrdb.NewMultiRemoveResponse(),
		}
	},
}

// MockCodec is only used for testing.
// By default it does nothing on marshalling and unmarshalling,
// thus it returns no error even if the input was ill-formed.
type MockCodec struct {
	mars   MarshalFunc
	unmars UnmarshalFunc
}

type UnmarshalFunc func(data []byte, v interface{}) error

type MarshalFunc func(v interface{}) ([]byte, error)

func (p *MockCodec) Marshal(v interface{}) ([]byte, error) {
	if p.mars != nil {
		return p.mars(v)
	}
	return nil, nil
}

func (p *MockCodec) Unmarshal(data []byte, v interface{}) error {
	if p.unmars != nil {
		return p.unmars(data, v)
	}
	return nil
}

func (p *MockCodec) String() string {
	return "mock"
}

func (p *MockCodec) MockMarshal(marshal MarshalFunc) {
	p.mars = marshal
}

func (p *MockCodec) MockUnMarshal(unmarshal UnmarshalFunc) {
	p.unmars = unmarshal
}

// a trait of the thrift-generated argument type (MetaQueryCfgArgs, RrdbPutArgs e.g.)
type rpcRequestArgs interface {
	String() string
	Write(oprot thrift.TProtocol) error
}

// a trait of the thrift-generated result type (MetaQueryCfgResult e.g.)
type rpcResponseResult interface {
	String() string
	Read(iprot thrift.TProtocol) error
}

type rpcCall struct {
	args   rpcRequestArgs
	result rpcResponseResult
	name   string // the rpc's name
	seqId  int32
	gpid   *base.Gpid
	rawReq []byte // the marshalled request in bytes
	err    error
}
