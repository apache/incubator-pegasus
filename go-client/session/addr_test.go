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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResolveMetaAddr(t *testing.T) {
	addrs := []string{"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"}
	resolvedAddrs, err := ResolveMetaAddr(addrs)
	assert.Nil(t, err)
	assert.Equal(t, addrs, resolvedAddrs)

	addrs = []string{"127.0.0.1:34601", "www.baidu.com", "127.0.0.1:34603"}
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)

	addrs = []string{"www.baidu.com"}
	_, err = ResolveMetaAddr(addrs)
	assert.Nil(t, err)
	assert.Greater(t, len(addrs), 0)

	addrs = []string{"abcde"}
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)

	addrs = nil
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)

	addrs = []string{}
	_, err = ResolveMetaAddr(addrs)
	assert.NotNil(t, err)
}
