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
	"encoding/binary"
	"fmt"
	"net"

	"github.com/apache/thrift/lib/go/thrift"
)

type RPCAddress struct {
	address int64
}

func NewRPCAddress(ip net.IP, port int) *RPCAddress {
	return &RPCAddress{
		address: (int64(binary.BigEndian.Uint32(ip.To4())) << 32) + (int64(port) << 16) + 1,
	}
}

func (a *RPCAddress) Read(iprot thrift.TProtocol) error {
	address, err := iprot.ReadI64()
	if err != nil {
		return err
	}
	a.address = address
	return nil
}

func (a *RPCAddress) Write(oprot thrift.TProtocol) error {
	return oprot.WriteI64(a.address)
}

func (a *RPCAddress) String() string {
	if a == nil {
		return "<nil>"
	}
	return fmt.Sprintf("RPCAddress(%s)", a.GetAddress())
}

func (a *RPCAddress) GetIP() net.IP {
	return net.IPv4(byte(0xff&(a.address>>56)), byte(0xff&(a.address>>48)), byte(0xff&(a.address>>40)), byte(0xff&(a.address>>32)))
}

func (a *RPCAddress) GetPort() int {
	return int(0xffff & (a.address >> 16))
}

func (a *RPCAddress) GetAddress() string {
	return fmt.Sprintf("%s:%d", a.GetIP(), a.GetPort())
}

func (a *RPCAddress) GetRawAddress() int64 {
	return a.address
}

func (a *RPCAddress) Equal(other *RPCAddress) bool {
	return a.address == other.address
}
