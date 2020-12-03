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

package util

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/XiaoMi/pegasus-go-client/idl/base"
)

type ResolveType int32

const (
	Host2Addr ResolveType = 0
	Addr2Host ResolveType = 1
)

// node's format, for example, pegasus.onebox.com:34801 or 127.0.0.1:34801
func Resolve(node string, resolveType ResolveType) (string, error) {
	splitResult := strings.Split(node, ":")
	if len(splitResult) < 2 {
		return node, fmt.Errorf("Invalid pegasus server node[%s]", node)
	}
	var ip = splitResult[0]
	var port = splitResult[1]

	var nodes []string
	switch resolveType {
	case Host2Addr:
		result, err := net.LookupHost(ip)
		if err != nil {
			return node, err
		}
		nodes = result
	case Addr2Host:
		result, err := net.LookupAddr(ip)
		if err != nil {
			return node, err
		}
		nodes = result
	}

	if len(nodes) == 0 || len(nodes) > 1 {
		return node, fmt.Errorf("Invalid pegasus server node(node resolve results = 0 or >1) [%s]", node)
	}
	ip = nodes[0]

	// Addr2Host result has suffix `.`, for example, `pegasus.onebox.com.` we need delete the suffix
	if resolveType == Addr2Host {
		ip = strings.TrimSuffix(nodes[0], ".")
	}
	return fmt.Sprintf("%s:%s", ip, port), nil
}

func Str2Gpid(gpid string) (*base.Gpid, error) {
	splitResult := strings.Split(gpid, ".")
	if len(splitResult) < 2 {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	appId, err := strconv.ParseInt(splitResult[0], 10, 32)

	if err != nil {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	partitionId, err := strconv.ParseInt(splitResult[1], 10, 32)
	if err != nil {
		return &base.Gpid{}, fmt.Errorf("Invalid gpid format [%s]", gpid)
	}

	return &base.Gpid{Appid: int32(appId), PartitionIndex: int32(partitionId)}, nil
}
