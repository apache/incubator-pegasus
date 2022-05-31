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

package executor

import (
	"context"
	"fmt"
	"hash/crc64"
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
)

var crc64Table = crc64.MakeTable(0x9a6c9329ac4bc9b5)

func crc64Hash(data []byte) uint64 {
	return crc64.Checksum(data, crc64Table)
}

func getPartitionIndex(hashKey []byte, partitionCount int) int32 {
	return int32(crc64Hash(hashKey) % uint64(partitionCount))
}

func PartitionIndex(rootCtx *Context, hashKeyStr string) error {
	pegasusArgs, err := readPegasusArgs(rootCtx, []string{hashKeyStr})
	if err != nil {
		return err
	}

	if rootCtx.UseTablePartitionCount == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := rootCtx.Meta.QueryConfig(ctx, rootCtx.UseTableName)
		if err != nil {
			return err
		}
		if resp.GetErr().Errno != base.ERR_OK.String() {
			return fmt.Errorf("QueryConfig failed: %s", resp.GetErr().String())
		}
		rootCtx.UseTablePartitionCount = int(resp.PartitionCount)
	}

	partIdx := getPartitionIndex(pegasusArgs[0], rootCtx.UseTablePartitionCount)
	fmt.Fprintf(rootCtx, "\nPartition index : %d\n", partIdx)

	return nil
}
