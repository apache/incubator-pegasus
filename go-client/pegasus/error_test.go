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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReturnCorrectErrorCode(t *testing.T) {
	client := NewClient(testingCfg)
	defer client.Close()

	tb, err := client.OpenTable(context.Background(), "temp")
	assert.Nil(t, err)
	defer tb.Close()

	err = tb.Set(context.Background(), nil, nil, nil)
	assert.Equal(t, err.(*PError).Op, OpSet)

	err = tb.Del(context.Background(), nil, nil)
	assert.Equal(t, err.(*PError).Op, OpDel)

	_, err = tb.Get(context.Background(), nil, nil)
	assert.Equal(t, err.(*PError).Op, OpGet)

	_, _, err = tb.MultiGet(context.Background(), nil, nil)
	assert.Equal(t, err.(*PError).Op, OpMultiGet)

	_, _, err = tb.MultiGetRange(context.Background(), nil, nil, nil)
	assert.Equal(t, err.(*PError).Op, OpMultiGetRange)

	err = tb.MultiSet(context.Background(), nil, nil, nil)
	assert.Equal(t, err.(*PError).Op, OpMultiSet)

	err = tb.MultiDel(context.Background(), nil, nil)
	assert.Equal(t, err.(*PError).Op, OpMultiDel)

	_, err = tb.TTL(context.Background(), nil, nil)
	assert.Equal(t, err.(*PError).Op, OpTTL)

	_, err = tb.Exist(context.Background(), nil, nil)
	assert.Equal(t, err.(*PError).Op, OpExist)

	_, err = tb.CheckAndSet(context.Background(), nil, nil, CheckTypeValueNotExist, nil, nil, nil,
		nil)
	assert.Equal(t, err.(*PError).Op, OpCheckAndSet)

	_, err = tb.SortKeyCount(context.Background(), nil)
	assert.Equal(t, err.(*PError).Op, OpSortKeyCount)

	_, err = tb.BatchGet(context.Background(), nil)
	assert.Equal(t, err.(*PError).Op, OpBatchGet)

	_, err = tb.Incr(context.Background(), nil, nil, 0)
	assert.Equal(t, err.(*PError).Op, OpIncr)

	_, err = tb.GetScanner(context.Background(), nil, nil, nil, nil)
	assert.Equal(t, err.(*PError).Op, OpGetScanner)

	_, err = tb.GetUnorderedScanners(context.Background(), 0, nil)
	assert.Equal(t, err.(*PError).Op, OpGetUnorderedScanners)

	err = tb.DelRange(context.Background(), nil, nil, nil)
	assert.Equal(t, err.(*PError).Op, OpDelRange)
}
