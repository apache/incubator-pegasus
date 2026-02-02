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
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRPCAddress(t *testing.T) {
	tests := []string{
		"127.0.0.1:8080",
		"192.168.0.1:123",
		"0.0.0.0:12345",
	}

	runner := func(test string) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			addr, err := net.ResolveTCPAddr("tcp", test)
			assert.NoError(t, err)

			rpcAddr := NewRPCAddress(addr.IP, addr.Port)
			assert.Equal(t, test, rpcAddr.GetAddress())
			assert.True(t, rpcAddr.Equal(rpcAddr))
		}
	}

	for _, test := range tests {
		t.Run(test, runner(test))
	}
}

func TestRPCAddressEquality(t *testing.T) {
	tests := []struct {
		x     string
		y     string
		equal bool
	}{
		{"127.0.0.1:8080", "127.0.0.1:8080", true},
		{"127.0.0.1:8080", "192.168.0.1:8080", false},
		{"127.0.0.1:8080", "127.0.0.1:8081", false},
	}

	runner := func(x string, y string, equal bool) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			addrX, err := net.ResolveTCPAddr("tcp", x)
			assert.NoError(t, err)

			addrY, err := net.ResolveTCPAddr("tcp", y)
			assert.NoError(t, err)

			rpcAddrX := NewRPCAddress(addrX.IP, addrX.Port)
			rpcAddrY := NewRPCAddress(addrY.IP, addrY.Port)

			assert.Equal(t, equal, rpcAddrX.Equal(rpcAddrY))
			assert.Equal(t, equal, rpcAddrY.Equal(rpcAddrX))
		}
	}

	for _, test := range tests {
		name := fmt.Sprintf("%s-vs-%s", test.x, test.y)
		t.Run(name, runner(test.x, test.y, test.equal))
	}
}
