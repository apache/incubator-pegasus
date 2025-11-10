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
	"fmt"
)

// PError is the return error type of all interfaces of pegasus client.
type PError struct {
	// Err is the error that occurred during the operation.
	Err error

	// The failed operation
	Op OpType
}

// OpType is the type of operation that led to PError.
type OpType int

// Operation types
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
	OpExist
	OpGetScanner
	OpGetUnorderedScanners
	OpNext
	OpScannerClose
	OpCheckAndSet
	OpSortKeyCount
	OpIncr
	OpBatchGet
	OpDelRange
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
	OpExist:                "EXIST",
	OpGetScanner:           "GET_SCANNER",
	OpGetUnorderedScanners: "GET_UNORDERED_SCANNERS",
	OpNext:                 "SCAN_NEXT",
	OpScannerClose:         "SCANNER_CLOSE",
	OpCheckAndSet:          "CHECK_AND_SET",
	OpSortKeyCount:         "SORTKEY_COUNT",
	OpIncr:                 "INCR",
	OpBatchGet:             "BATCH_GET",
	OpDelRange:             "DEL_RANGE",
}

func (op OpType) String() string {
	return opTypeToStringMap[op]
}

func (e *PError) Error() string {
	return fmt.Sprintf("pegasus %s failed: %s", e.Op, e.Err.Error())
}
