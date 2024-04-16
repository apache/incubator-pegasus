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

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import org.junit.jupiter.api.Test;

public class TestHostPort {

  @Test
  public void testHostPortReadAndWrite() throws Exception {
    host_port hostPort = new host_port();

    assertNotNull(hostPort);
    assertNull(hostPort.getHost());
    assertEquals(hostPort.getPort(), 0);
    assertEquals(hostPort.getHostPortType(), 0);

    hostPort.setHost("www.baidu.com");
    hostPort.setPort(65535);
    hostPort.setHostPortType((byte) 1);

    TMemoryBuffer buffer = new TMemoryBuffer(500);
    TBinaryProtocol protocol = new TBinaryProtocol(buffer);
    hostPort.write(protocol);

    host_port decodeHostPort = new host_port();
    decodeHostPort.read(protocol);

    assertEquals(hostPort.getHost(), decodeHostPort.getHost());
    assertEquals(hostPort.getPort(), decodeHostPort.getPort());
    assertEquals(hostPort.getHostPortType(), decodeHostPort.getHostPortType());
  }
}
