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

package org.apache.pegasus.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.pegasus.rpc.async.HostNameResolver;
import org.junit.jupiter.api.Test;

public class TestRpcAddress {
  @Test
  public void testResolveFromHostPort() throws Exception {
    HostNameResolver hostNameResolver = new HostNameResolver();
    rpc_address[] addrs = hostNameResolver.resolve("127.0.0.1:34601");

    assertNotNull(addrs);
    assertEquals(addrs.length, 1);
    assertEquals(addrs[0].get_ip(), "127.0.0.1");
    assertEquals(addrs[0].get_port(), 34601);

    addrs = hostNameResolver.resolve("www.baidu.com:80");
    assertNotNull(addrs);
    assertTrue(addrs.length >= 1);

    addrs = hostNameResolver.resolve("abcabcabcabc:34601");
    assertNull(addrs);

    try {
      addrs = hostNameResolver.resolve("localhost");
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
      assertNull(addrs);
    }
  }

  @Test
  public void testFromString() throws Exception {
    rpc_address addr = new rpc_address();
    assertTrue(addr.fromString("127.0.0.1:34601"));
    assertEquals(addr.get_ip(), "127.0.0.1");
    assertEquals(addr.get_port(), 34601);
  }
}
