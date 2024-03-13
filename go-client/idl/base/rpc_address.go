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

func (r *RPCAddress) Read(iprot thrift.TProtocol) error {
	address, err := iprot.ReadI64()
	if err != nil {
		return err
	}
	r.address = address
	return nil
}

func (r *RPCAddress) Write(oprot thrift.TProtocol) error {
	return oprot.WriteI64(r.address)
}

func (r *RPCAddress) String() string {
	if r == nil {
		return "<nil>"
	}
	return fmt.Sprintf("RPCAddress(%s)", r.GetAddress())
}

func (r *RPCAddress) GetIP() net.IP {
	return net.IPv4(byte(0xff&(r.address>>56)), byte(0xff&(r.address>>48)), byte(0xff&(r.address>>40)), byte(0xff&(r.address>>32)))
}

func (r *RPCAddress) GetPort() int {
	return int(0xffff & (r.address >> 16))
}

func (r *RPCAddress) GetAddress() string {
	return fmt.Sprintf("%s:%d", r.GetIP(), r.GetPort())
}

func (r *RPCAddress) GetRawAddress() int64 {
	return r.address
}
