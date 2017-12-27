// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"fmt"

	"github.com/pegasus-kv/pegasus-go-client/idl/base"
)

type PError struct {
	// Err is the error that occurred during the operation.
	Err error

	// Code is the error code that identifies the type of this error.
	Code base.ErrType

	// Op is the operation that caused this error
	Op OpType
}

type OpType int

const (
	OpQueryConfig OpType = iota
	OpGet
	OpSet
	OpDel
	OpClose
)

var opTypeToStringMap = map[OpType]string{
	OpQueryConfig: "table configuration query",
	OpGet:         "GET",
	OpSet:         "SET",
	OpDel:         "DEL",
	OpClose:       "Close",
}

func (op OpType) String() string {
	return opTypeToStringMap[op]
}

func (e *PError) Error() string {
	estr := fmt.Sprintf("pegasus-client %s failed: [%s]", e.Op, e.Code)
	if e.Err != nil {
		estr += " " + e.Err.Error()
	}
	return estr
}

func newPError(err error, code base.ErrType) *PError {
	return &PError{Err: err, Code: code}
}

func newPErrorGpid(err error, code base.ErrType, gpid *base.Gpid) *PError {
	return &PError{Err: &replicaError{
		err:  err,
		gpid: gpid,
	}, Code: code}
}

type replicaError struct {
	err  error
	gpid *base.Gpid
}

func (re *replicaError) Error() string {
	return re.gpid.String() + " " + re.err.Error()
}
