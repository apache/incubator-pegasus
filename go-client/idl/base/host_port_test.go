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
	"testing"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

func stringify(host string, port uint16) string {
	return fmt.Sprintf("<%s:%d>", host, port)
}

func TestHostPort(t *testing.T) {
	tests := map[string]uint16{
		"localhost":          8080,
		"pegasus.apache.org": 443,
	}

	runner := func(host string, port uint16) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			hp := NewHostPort(host, port)
			assert.Equal(t, host, hp.GetHost())
			assert.Equal(t, port, hp.GetPort())
			assert.True(t, hp.Equal(hp))

			// Test serialization.
			buf := thrift.NewTMemoryBuffer()
			oprot := thrift.NewTBinaryProtocolTransport(buf)
			assert.NoError(t, hp.Write(oprot))

			// Test deserialization.
			peer := NewHostPort("", 0)
			assert.NoError(t, peer.Read(oprot))
			assert.True(t, peer.Equal(peer))

			// Test equality.
			assert.Equal(t, hp, peer)
			assert.True(t, hp.Equal(peer))
			assert.True(t, peer.Equal(hp))
		}
	}

	for host, port := range tests {
		t.Run(stringify(host, port), runner(host, port))
	}
}

func TestHostPortEquality(t *testing.T) {
	type hpCase struct {
		host string
		port uint16
	}
	type testCase struct {
		x     hpCase
		y     hpCase
		equal bool
	}
	tests := []testCase{
		{hpCase{"localhost", 8080}, hpCase{"localhost", 8080}, true},
		{hpCase{"localhost", 8080}, hpCase{"pegasus.apache.org", 8080}, false},
		{hpCase{"localhost", 8080}, hpCase{"localhost", 8081}, false},
	}

	testName := func(hpX hpCase, hpY hpCase) string {
		hpName := func(hp hpCase) string {
			return stringify(hp.host, hp.port)
		}
		return fmt.Sprintf("%s-vs-%s", hpName(hpX), hpName(hpY))
	}

	runner := func(test testCase) func(t *testing.T) {
		return func(t *testing.T) {
			t.Parallel()

			hpX := NewHostPort(test.x.host, test.x.port)
			hpY := NewHostPort(test.y.host, test.y.port)

			assert.Equal(t, test.equal, hpX.Equal(hpY))
			assert.Equal(t, test.equal, hpY.Equal(hpX))
		}
	}

	for _, test := range tests {
		t.Run(testName(test.x, test.y), runner(test))
	}
}
