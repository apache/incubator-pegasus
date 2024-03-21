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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import org.apache.pegasus.apps.update_request;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.operator.client_operator;
import org.apache.pegasus.rpc.InternalTableOptions;
import org.apache.pegasus.rpc.ReplicationException;
import org.apache.pegasus.tools.Toollet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;

public class TableHandlerTest {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TableHandlerTest.class);

  private String addr_list = "127.0.0.1:34601, 127.0.0.1:34602, 127.0.0.1:34603";
  private String[] replica_servers = {"127.0.0.1:34801", "127.0.0.1:34802", "127.0.01:34803"};

  private ClusterManager testManager;

  @BeforeEach
  public void before() throws Exception {
    testManager = new ClusterManager(ClientOptions.builder().metaServers(addr_list).build());
  }

  @AfterEach
  public void after() throws Exception {}

  private rpc_address getValidWrongServer(final rpc_address right_address) {
    ArrayList<rpc_address> replicas = new ArrayList<rpc_address>();
    for (int i = 0; i < replica_servers.length; ++i) {
      rpc_address a = new rpc_address();
      boolean ans = a.fromString(replica_servers[i]);
      assert ans;

      if (a.get_port() != right_address.get_port()) replicas.add(a);
    }

    int p = (int) (Math.random() * replicas.size());
    return replicas.get(p);
  }

  /** Method: operate(client_operator op) */
  @Test
  public void testOperateOp() throws Exception {
    System.out.println("TableHandlerTest#testOperateOp");
    TableHandler table = null;
    try {
      table = testManager.openTable("temp", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      fail();
    }
    assertNotNull(table);

    update_request request = new update_request();
    request.key = new blob("hello".getBytes());
    request.value = new blob("value".getBytes());

    long partitionHash = table.getHash(request.key.data);
    final gpid pid = table.getGpidByHash(partitionHash);

    TableHandler.ReplicaConfiguration handle = table.getReplicaConfig(pid.get_pidx());

    // 1. modify the replica handler to a not exist one
    final rpc_address old_addr = handle.primarySession.getAddress();
    logger.info("the right primary for {} is {}", pid.toString(), old_addr.toString());

    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:123");
    handle.ballot--;
    handle.primarySession = testManager.getReplicaSession(addr);

    client_operator op = new Toollet.test_operator(pid, request);

    try {
      table.operate(op, 0);
      fail();
    } catch (ReplicationException ex) {
      logger.info("timeout is set 0, no enough time to process");
      assertEquals(error_code.error_types.ERR_TIMEOUT, ex.getErrorType());
    }

    // we should try to query meta since the session to replica-server is unreachable.
    final TableHandler finalTableRef = table;
    boolean ans =
        Toollet.waitCondition(
            new Toollet.BoolCallable() {
              @Override
              public boolean call() {
                ReplicaSession session =
                    finalTableRef.getReplicaConfig(pid.get_pidx()).primarySession;
                if (session == null) return false;
                return session.getAddress().equals(old_addr);
              }
            },
            10);
    assertTrue(ans);

    // 2. set an invalid task code, server should not response
    op = new Toollet.test_operator(pid, request);
    try {
      table.operate(op, 0);
      fail();
    } catch (ReplicationException ex) {
      assertEquals(error_code.error_types.ERR_TIMEOUT, ex.getErrorType());
    }

    // 3. we should open a onebox cluster with three replica servers. thus every
    // server will server all the replicas. Then we can test query a request to secondary
    handle = table.getReplicaConfig(pid.get_pidx());
    addr = getValidWrongServer(old_addr);
    logger.info("the wrong valid server is {}", addr.toString());

    assertFalse(addr.equals(old_addr));
    handle.ballot--;
    handle.primarySession = testManager.getReplicaSession(addr);

    op = new Toollet.test_operator(pid, request);
    try {
      table.operate(op, 0);
      fail();
    } catch (ReplicationException ex) {
      assertEquals(error_code.error_types.ERR_TIMEOUT, ex.getErrorType());
    }
  }

  /** Method: tryQueryMeta(final gpid pid, final long signature) */
  @Test
  public void testTryQueryMeta() throws Exception {
    System.out.println("test try query meta");
    TableHandler table = null;

    try {
      table = testManager.openTable("temp", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      fail();
    }
    assertNotNull(table);
    assertEquals(8, table.getPartitionCount());

    TableHandler.TableConfiguration tableConfig = table.tableConfig_.get();
    for (int i = 0; i < tableConfig.replicas.size(); ++i) {
      TableHandler.ReplicaConfiguration handle = tableConfig.replicas.get(i);
      assertNotNull(handle);
      assertNotNull(handle.primarySession);
    }

    // mark a handler to inactive
    TableHandler.ReplicaConfiguration handle = tableConfig.replicas.get(0);
    long oldBallot = handle.ballot - 1;
    handle.ballot = oldBallot;
    handle.primarySession = null;

    boolean doTheQuerying = table.tryQueryMeta(tableConfig.updateVersion);
    assertTrue(doTheQuerying);

    final TableHandler finalRef = table;
    assertTrue(
        Toollet.waitCondition(
            new Toollet.BoolCallable() {
              @Override
              public boolean call() {
                return finalRef.getReplicaConfig(0).primarySession != null;
              }
            },
            10));

    handle = table.getReplicaConfig(0);
    assertEquals(oldBallot + 1, handle.ballot);
  }

  @Test
  public void testConnectAfterQueryMeta() throws Exception {
    System.out.println("TableHandlerTest#testConnectAfterQueryMeta");
    TableHandler table = null;

    try {
      table = testManager.openTable("temp", InternalTableOptions.forTest());
    } catch (ReplicationException e) {
      fail();
    }
    assertNotNull(table);

    Thread.sleep(100);
    ArrayList<TableHandler.ReplicaConfiguration> replicas = table.tableConfig_.get().replicas;
    for (TableHandler.ReplicaConfiguration r : replicas) {
      assertEquals(r.primarySession.getState(), ReplicaSession.ConnState.CONNECTED);
    }
  }
}
