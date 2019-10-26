// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.base.rpc_address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/*
 * Resolves host:port into a set of ip addresses.
 * The intention of this class is to mock DNS.
 */
public class HostNameResolver {

  public rpc_address[] resolve(String hostPort) throws IllegalArgumentException {
    String[] pairs = hostPort.split(":");
    if (pairs.length != 2) {
      throw new IllegalArgumentException("Meta server host name format error!");
    }

    try {
      Integer port = Integer.valueOf(pairs[1]);
      InetAddress[] resolvedAddresses = InetAddress.getAllByName(pairs[0]);
      rpc_address[] results = new rpc_address[resolvedAddresses.length];
      int size = 0;
      for (InetAddress addr : resolvedAddresses) {
        rpc_address rpcAddr = new rpc_address();
        int ip = ByteBuffer.wrap(addr.getAddress()).order(ByteOrder.BIG_ENDIAN).getInt();
        rpcAddr.address = ((long) ip << 32) + ((long) port << 16) + 1;
        results[size++] = rpcAddr;
      }
      return results;
    } catch (UnknownHostException e) {
      return null;
    }
  }
}
