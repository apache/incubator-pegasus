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

package base

import (
	"fmt"
	"reflect"

	"github.com/apache/thrift/lib/go/thrift"
)

// ErrorCode / Primitive for Pegasus thrift framework.
type ErrorCode struct {
	Errno string
}

// How to generate the map from string to error codes?
// First:
//  - go get github.com/alvaroloes/enumer
// Second:
//  - cd idl/base
//  - enumer -type=DsnErrCode -output=dsn_err_string.go

//go:generate enumer -type=DsnErrCode -output=err_type_string.go
type DsnErrCode int32

const (
	ERR_OK DsnErrCode = iota
	ERR_UNKNOWN
	ERR_REPLICATION_FAILURE
	ERR_APP_EXIST
	ERR_APP_NOT_EXIST
	ERR_APP_DROPPED
	ERR_BUSY_CREATING
	ERR_BUSY_DROPPING
	ERR_EXPIRED
	ERR_LOCK_ALREADY_EXIST
	ERR_HOLD_BY_OTHERS
	ERR_RECURSIVE_LOCK
	ERR_NO_OWNER
	ERR_NODE_ALREADY_EXIST
	ERR_INCONSISTENT_STATE
	ERR_ARRAY_INDEX_OUT_OF_RANGE
	ERR_SERVICE_NOT_FOUND
	ERR_SERVICE_ALREADY_RUNNING
	ERR_IO_PENDING
	ERR_TIMEOUT
	ERR_SERVICE_NOT_ACTIVE
	ERR_BUSY
	ERR_NETWORK_INIT_FAILED
	ERR_FORWARD_TO_OTHERS
	ERR_OBJECT_NOT_FOUND
	ERR_HANDLER_NOT_FOUND
	ERR_LEARN_FILE_FAILED
	ERR_GET_LEARN_STATE_FAILED
	ERR_INVALID_VERSION
	ERR_INVALID_PARAMETERS
	ERR_CAPACITY_EXCEEDED
	ERR_INVALID_STATE
	ERR_INACTIVE_STATE
	ERR_NOT_ENOUGH_MEMBER
	ERR_FILE_OPERATION_FAILED
	ERR_HANDLE_EOF
	ERR_WRONG_CHECKSUM
	ERR_INVALID_DATA
	ERR_INVALID_HANDLE
	ERR_INCOMPLETE_DATA
	ERR_VERSION_OUTDATED
	ERR_PATH_NOT_FOUND
	ERR_PATH_ALREADY_EXIST
	ERR_ADDRESS_ALREADY_USED
	ERR_STATE_FREEZED
	ERR_LOCAL_APP_FAILURE
	ERR_BIND_IOCP_FAILED
	ERR_NETWORK_START_FAILED
	ERR_NOT_IMPLEMENTED
	ERR_CHECKPOINT_FAILED
	ERR_WRONG_TIMING
	ERR_NO_NEED_OPERATE
	ERR_CORRUPTION
	ERR_TRY_AGAIN
	ERR_CLUSTER_NOT_FOUND
	ERR_CLUSTER_ALREADY_EXIST
	ERR_SERVICE_ALREADY_EXIST
	ERR_INJECTED
	ERR_NETWORK_FAILURE
	ERR_UNDER_RECOVERY
	ERR_OPERATION_DISABLED
	ERR_ZOOKEEPER_OPERATION
	ERR_CHILD_REGISTERED
	ERR_INGESTION_FAILED
	ERR_UNAUTHENTICATED
	ERR_KRB5_INTERNAL
	ERR_SASL_INTERNAL
	ERR_SASL_INCOMPLETE
	ERR_ACL_DENY
	ERR_SPLITTING
	ERR_PARENT_PARTITION_MISUSED
	ERR_CHILD_NOT_READY
	ERR_DISK_INSUFFICIENT
)

func (e DsnErrCode) Error() string {
	return fmt.Sprintf("[%s]", e.String())
}

func (ec *ErrorCode) Read(iprot thrift.TProtocol) (err error) {
	ec.Errno, err = iprot.ReadString()
	return
}

func (ec *ErrorCode) Write(oprot thrift.TProtocol) error {
	return oprot.WriteString(ec.Errno)
}

func (ec *ErrorCode) String() string {
	if ec == nil {
		return "<nil>"
	}
	return fmt.Sprintf("ErrorCode(%+v)", *ec)
}

type baseError struct {
	message string
}

// Implement error interface.
func (e *baseError) Error() string {
	if e == nil || e.message == ERR_OK.String() {
		return ERR_OK.String()
	}
	return e.message
}

// Convert ErrorCode to error.
func (ec *ErrorCode) AsError() error {
	if ec == nil || ec.Errno == ERR_OK.String() {
		return nil
	}
	return &baseError{
		message: ec.Errno,
	}
}

// `resp` is the thrift-generated response struct of RPC.
func GetResponseError(resp interface{}) error {
	result := reflect.ValueOf(resp).MethodByName("GetErr").Call([]reflect.Value{})
	iec := result[0].Interface()
	if iec == nil {
		return nil
	}

	return iec.(*ErrorCode).AsError()
}

//go:generate enumer -type=RocksDBErrCode -output=rocskdb_err_string.go
type RocksDBErrCode int32

const (
	Ok RocksDBErrCode = iota
	NotFound
	Corruption
	NotSupported
	InvalidArgument
	IOError
	MergeInProgress
	Incomplete
	ShutdownInProgress
	TimedOut
	Aborted
	Busy
	Expired
	TryAgain
)

func NewRocksDBErrFromInt(e int32) error {
	err := RocksDBErrCode(e)
	if err == Ok {
		return nil
	}
	return err
}

func (e RocksDBErrCode) Error() string {
	return fmt.Sprintf("ROCSKDB_ERR(%s)", e.String())
}
