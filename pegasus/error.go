// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package pegasus

import (
	"fmt"
)

type PError struct {
	// Err is the error that occurred during the operation.
	Err error

	// Op is the operation that caused this error
	Op OpType
}

type OpType int

const (
	OpQueryConfig OpType = iota
	OpGet
	OpSet
	OpDel
	OpMultiDel
	OpMultiGet
	OpMultiGetRange
	OpClose
	OpMultiSet
	OpTTL
	OpGetScanner
	OpGetUnorderedScanners
	OpNext
	OpScannerClose
)

var opTypeToStringMap = map[OpType]string{
	OpQueryConfig:          "table configuration query",
	OpGet:                  "GET",
	OpSet:                  "SET",
	OpDel:                  "DEL",
	OpMultiGet:             "MULTI_GET",
	OpMultiGetRange:        "MULTI_GET_RANGE",
	OpMultiDel:             "MULTI_DEL",
	OpClose:                "Close",
	OpMultiSet:             "MULTI_SET",
	OpTTL:                  "TTL",
	OpGetScanner:           "GET_SCANNER",
	OpGetUnorderedScanners: "GET_UNORDERED_SCANNERS",
	OpNext:                 "SCAN_NEXT",
	OpScannerClose:         "SCANNER_CLOSE",
}

func (op OpType) String() string {
	return opTypeToStringMap[op]
}

func (e *PError) Error() string {
	return fmt.Sprintf("pegasus-go-client %s failed: %s", e.Op, e.Err.Error())
}
