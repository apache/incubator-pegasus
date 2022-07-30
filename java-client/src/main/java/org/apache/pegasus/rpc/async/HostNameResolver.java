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
package org.apache.pegasus.rpc.async;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.apache.pegasus.base.rpc_address;

/*
 * Resolves host:port into a set of ip addresses.
 * The intention of this class is to mock DNS.
 */
public class HostNameResolver {

  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(HostNameResolver.class);

  public rpc_address[] resolve(String hostPort) throws IllegalArgumentException {
    String[] pairs = hostPort.split(":");
    if (pairs.length != 2) {
      throw new IllegalArgumentException("Meta server host name format error!");
    }

    try {
      Integer port = Integer.valueOf(pairs[1]);
      logger.info("start to resolve hostname {} into ip addresses", pairs[0]);
      InetAddress[] resolvedAddresses = InetAddress.getAllByName(pairs[0]);
      rpc_address[] results = new rpc_address[resolvedAddresses.length];
      int size = 0;
      for (InetAddress addr : resolvedAddresses) {
        rpc_address rpcAddr = new rpc_address();
        int ip = ByteBuffer.wrap(addr.getAddress()).order(ByteOrder.BIG_ENDIAN).getInt();
        rpcAddr.address = ((long) ip << 32) + ((long) port << 16) + 1;
        logger.info("resolved ip address {} from host {}", rpcAddr, pairs[0]);
        results[size++] = rpcAddr;
      }
      return results;
    } catch (UnknownHostException e) {
      return null;
    }
  }
}
