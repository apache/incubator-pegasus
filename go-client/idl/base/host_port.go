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

	"github.com/apache/thrift/lib/go/thrift"
)

type HostPortType int32

const (
	HOST_TYPE_INVALID HostPortType = iota
	HOST_TYPE_IPV4
	HOST_TYPE_GROUP
)

type HostPort struct {
	host string
	port uint16
	// TODO(yingchun): Now only support ipv4
	hpType HostPortType
}

func NewHostPort(host string, port uint16) *HostPort {
	return &HostPort{
		host:   host,
		port:   port,
		hpType: HOST_TYPE_IPV4,
	}
}

func (hp *HostPort) Read(iprot thrift.TProtocol) error {
	host, err := iprot.ReadString()
	if err != nil {
		return err
	}
	port, err := iprot.ReadI16()
	if err != nil {
		return err
	}
	hpType, err := iprot.ReadByte()
	if err != nil {
		return err
	}

	hp.host = host
	hp.port = uint16(port)
	hp.hpType = HostPortType(hpType)
	return nil
}

func (hp *HostPort) Write(oprot thrift.TProtocol) error {
	err := oprot.WriteString(hp.host)
	if err != nil {
		return err
	}
	err = oprot.WriteI16(int16(hp.port))
	if err != nil {
		return err
	}
	err = oprot.WriteByte(int8(hp.hpType))
	if err != nil {
		return err
	}
	return nil
}

func (hp *HostPort) GetHost() string {
	return hp.host
}

func (hp *HostPort) GetPort() uint16 {
	return hp.port
}

func (hp *HostPort) String() string {
	if hp == nil {
		return "<nil>"
	}
	return fmt.Sprintf("HostPort(%s:%d)", hp.host, hp.port)
}

func (hp *HostPort) GetHostPort() string {
	return fmt.Sprintf("%s:%d", hp.host, hp.port)
}

func (hp *HostPort) Equal(other *HostPort) bool {
	if hp == other {
		return true
	}

	if hp == nil || other == nil {
		return false
	}

	if hp.hpType != other.hpType {
		return false
	}

	switch hp.hpType {
	case HOST_TYPE_IPV4:
		return hp.host == other.host &&
			hp.port == other.port
	case HOST_TYPE_GROUP:
		// TODO(wangdan): support HOST_TYPE_GROUP.
		return false
	default:
		return true
	}
}
