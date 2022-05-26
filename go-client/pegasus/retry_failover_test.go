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
	"time"

	"github.com/apache/incubator-pegasus/go-client/idl/base"
	"github.com/stretchr/testify/assert"
)

func TestRetryFailOver_Success(t *testing.T) {
	// success, no retry
	times := 0
	_, err := retryFailOver(context.Background(), func() (confUpdated bool, result interface{}, retry bool, err error) {
		times++
		return false, nil, false, nil
	})
	assert.NoError(t, err)
	assert.Equal(t, times, 1)
}

func TestRetryFailOver_FailureNoRetry(t *testing.T) {
	// first try fails, but it shouldn't retry
	times := 0
	_, err := retryFailOver(context.Background(), func() (confUpdated bool, result interface{}, retry bool, err error) {
		times++
		return false, nil, false, context.DeadlineExceeded
	})
	assert.Error(t, err)
	assert.Equal(t, times, 1)
}

func TestRetryFailOver_FailureRetryWithIncreasingInterval(t *testing.T) {
	// fail n times, success at the (n+1)th attempt
	start := time.Now()
	var elapses []time.Duration
	_, err := retryFailOver(context.Background(), func() (confUpdated bool, result interface{}, retry bool, err error) {
		elapses = append(elapses, time.Since(start))
		start = time.Now()
		if len(elapses) > 5 {
			return false, nil, false, nil
		}
		return true, nil, true, base.ERR_OBJECT_NOT_FOUND
	})
	assert.NoError(t, err)
	assert.Equal(t, len(elapses), 6)
	for i := 4; i < len(elapses); i++ {
		// ensure the backoff interval is basically increasing
		assert.GreaterOrEqual(t, int64(elapses[i]), int64(elapses[1]))
	}
}

func TestRetryFailOver_FailureRetryUntilTimeout(t *testing.T) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	res, err := retryFailOver(ctx, func() (confUpdated bool, result interface{}, retry bool, err error) {
		return true, nil, true, base.ERR_OBJECT_NOT_FOUND
	})
	elapsed := time.Since(start)

	assert.Error(t, err)
	assert.Equal(t, err, context.DeadlineExceeded)
	assert.Nil(t, res)
	assert.GreaterOrEqual(t, int64(elapsed), int64(time.Second*5))
}
