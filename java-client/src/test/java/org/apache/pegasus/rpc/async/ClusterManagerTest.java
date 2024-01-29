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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.fail;

import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.rpc.InternalTableOptions;
import org.apache.pegasus.rpc.ReplicationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ClusterManagerTest {
  @BeforeEach
  public void before() throws Exception {}

  @AfterEach
  public void after() throws Exception {}

  /** Method: getReplicaSession(rpc_address address) */
  @Test
  public void testGetReplicaSession() throws Exception {
    String address_list = "127.0.0.1:1,127.0.0.1:2,127.0.0.1:3";

    ClusterManager testManager =
        new ClusterManager(ClientOptions.builder().metaServers(address_list).build());

    // input an invalid rpc address
    rpc_address address = new rpc_address();
    ReplicaSession session = testManager.getReplicaSession(address);
    assertNull(session);
  }

  /** Method: openTable(String name, KeyHasher h) */
  @Test
  public void testOpenTable() throws Exception {
    // test invalid meta list
    String address_list = "127.0.0.1:123,127.0.0.1:124,127.0.0.1:125";
    ClusterManager testManager =
        new ClusterManager(ClientOptions.builder().metaServers(address_list).build());

    TableHandler result = null;
    try {
      result = testManager.openTable("testName", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      assertEquals(error_code.error_types.ERR_SESSION_RESET, e.getErrorType());
    } finally {
      assertNull(result);
    }
    testManager.close();

    // test partially invalid meta list
    String address_list2 = "127.0.0.1:123,127.0.0.1:34603,127.0.0.1:34601,127.0.0.1:34602";
    testManager = new ClusterManager(ClientOptions.builder().metaServers(address_list2).build());
    try {
      result = testManager.openTable("hehe", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      assertEquals(error_code.error_types.ERR_OBJECT_NOT_FOUND, e.getErrorType());
    } finally {
      assertNull(result);
    }

    // test open an valid table
    try {
      result = testManager.openTable("temp", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      fail();
    } finally {
      assertNotNull(result);
      // in onebox, we create a table named temp with 8 partitions in default.
      assertEquals(8, result.getPartitionCount());
    }
    testManager.close();
  }
}
