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

package session

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	"github.com/apache/incubator-pegasus/go-client/rpc"
	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
)

type timeoutReader struct {
	readTimes int
}

type timeoutError struct {
	*net.OpError
}

func (*timeoutError) Timeout() bool {
	return true
}

func (*timeoutError) Error() string {
	return "i/o timeout"
}

func (r *timeoutReader) Read([]byte) (n int, err error) {
	if r.readTimes == 0 {
		r.readTimes++
		return 0, &net.OpError{Err: &timeoutError{}}
	}
	return 0, io.EOF
}

func TestNodeSession_ReadTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	{ // irrelevant test, only to ensure timeoutReader returns network timeout error.
		reader := timeoutReader{}
		_, err := reader.Read(nil)
		assert.True(t, rpc.IsNetworkTimeoutErr(err))
	}

	unresponsiveHandlerCalled := false
	n := newFakeNodeSession(&timeoutReader{}, bytes.NewBuffer(make([]byte, 0)))
	n.unresponsiveHandler = func(s NodeSession) {
		unresponsiveHandlerCalled = true
	}
	n.lastWriteTime = time.Now().UnixNano()

	err := n.loopForResponse() // since the timeoutReader returns EOF at last, the loop will finally terminate
	assert.Nil(t, err)
	assert.True(t, unresponsiveHandlerCalled)
}

func TestNodeSession_HasRecentUnresponsiveWrite(t *testing.T) {
	n := newFakeNodeSession(bytes.NewBuffer(make([]byte, 0)), bytes.NewBuffer(make([]byte, 0)))
	n.lastWriteTime = time.Now().UnixNano()
	assert.True(t, n.hasRecentUnresponsiveWrite())

	n.lastWriteTime = int64(time.Now().Add(-1 * time.Minute).UnixNano()) // this write is not recent enough
	assert.False(t, n.hasRecentUnresponsiveWrite())
}
