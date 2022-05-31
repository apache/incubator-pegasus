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
	"fmt"
	"net"
)

// ResolveMetaAddr into a list of TCP4 addresses. Error is returned if the given `addrs` are not either
// a list of valid TCP4 addresses, or a resolvable hostname.
func ResolveMetaAddr(addrs []string) ([]string, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("meta server list should not be empty")
	}

	// case#1: all addresses are in TCP4 already
	allTCPAddr := true
	for _, addr := range addrs {
		_, err := net.ResolveTCPAddr("tcp4", addr)
		if err != nil {
			allTCPAddr = false
			break
		}
	}
	if allTCPAddr {
		return addrs, nil
	}

	// case#2: address is a hostname
	if len(addrs) == 1 {
		actualAddrs, err := net.LookupHost(addrs[0])
		if err == nil {
			return actualAddrs, nil
		}
	}

	return nil, fmt.Errorf("illegal meta addresses: %s", addrs)
}
