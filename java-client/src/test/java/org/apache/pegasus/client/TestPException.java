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

package org.apache.pegasus.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.util.concurrent.DefaultPromise;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.pegasus.apps.update_request;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.client.PegasusTable.Request;
import org.apache.pegasus.operator.rrdb_put_operator;
import org.apache.pegasus.rpc.InternalTableOptions;
import org.apache.pegasus.rpc.async.ClusterManager;
import org.apache.pegasus.rpc.async.TableHandler;
import org.junit.jupiter.api.Test;

public class TestPException {
  private String metaList = "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
  private Request request = new Request("hashKey".getBytes(), "sortKey".getBytes());

  @Test
  public void testThreadInterrupted() throws Exception {
    PException ex = PException.threadInterrupted("test", new InterruptedException("intxxx"));
    String exceptionInfo =
        "{version}: org.apache.pegasus.rpc.ReplicationException: ERR_THREAD_INTERRUPTED: [table=test] Thread was interrupted: intxxx";
    assertEquals(exceptionInfo, ex.getMessage());
  }

  @Test
  public void testTimeout() throws Exception {
    PException ex =
        PException.timeout(metaList, "test", request, 1000, new TimeoutException("tmxxx"));
    String exceptionInfo =
        String.format(
            "{version}: org.apache.pegasus.rpc.ReplicationException: ERR_TIMEOUT: [metaServer=%s, table=test, request=%s, timeout=1000ms] Timeout on Future await: tmxxx",
            metaList, request.toString());
    assertEquals(exceptionInfo, ex.getMessage());
  }

  @Test
  public void testVersion() {
    // Test the constructors of PException

    PException ex = new PException("test");
    assertEquals("{version}: test", ex.getMessage());

    ex = new PException("test", new TimeoutException());
    assertEquals("{version}: test", ex.getMessage());
  }

  @Test
  public void testHandleReplicationException() throws Exception {
    String metaList = "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers(metaList).build());
    TableHandler table = manager.openTable("temp", InternalTableOptions.forTest());
    DefaultPromise<Void> promise = table.newPromise();
    update_request req = new update_request(new blob(), new blob(), 100);
    gpid gpid = table.getGpidByHash(1);
    rrdb_put_operator op = new rrdb_put_operator(gpid, table.getTableName(), req, 0);
    op.rpc_error.errno = error_code.error_types.ERR_OBJECT_NOT_FOUND;

    // set failure in promise, the exception is thrown as ExecutionException.
    int timeout = 1000;
    PegasusClient client = (PegasusClient) PegasusClientFactory.getSingletonClient();
    PegasusTable pegasusTable = new PegasusTable(client, table);
    pegasusTable.handleReplicaException(request, promise, op, table, timeout);
    try {
      promise.get();
    } catch (ExecutionException e) {
      TableHandler.ReplicaConfiguration replicaConfig = table.getReplicaConfig(gpid.get_pidx());
      String server =
          replicaConfig.primaryAddress.get_ip() + ":" + replicaConfig.primaryAddress.get_port();

      String msg =
          String.format(
              "org.apache.pegasus.client.PException: {version}: org.apache.pegasus.rpc.ReplicationException: ERR_OBJECT_NOT_FOUND: [metaServer=%s,table=temp,operation=put,request=%s,replicaServer=%s,gpid=(%s),timeout=%dms] The replica server doesn't serve this partition!",
              client.getMetaList(), request.toString(), server, gpid.toString(), timeout);
      assertEquals(e.getMessage(), msg);
      return;
    } catch (InterruptedException e) {
      fail();
    }
    fail();
  }

  @Test
  public void testTimeOutIsZero() throws Exception {
    // ensure "PException ERR_TIMEOUT" is thrown with the real timeout value, when user given
    // timeout is 0.
    String metaList = "127.0.0.1:34601,127.0.0.1:34602, 127.0.0.1:34603";
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers(metaList).build());
    TableHandler table = manager.openTable("temp", InternalTableOptions.forTest());
    DefaultPromise<Void> promise = table.newPromise();
    update_request req = new update_request(new blob(), new blob(), 100);
    gpid gpid = table.getGpidByHash(1);
    rrdb_put_operator op = new rrdb_put_operator(gpid, table.getTableName(), req, 0);
    op.rpc_error.errno = error_code.error_types.ERR_TIMEOUT;

    PegasusClient client = (PegasusClient) PegasusClientFactory.getSingletonClient();
    PegasusTable pegasusTable = new PegasusTable(client, table);
    pegasusTable.handleReplicaException(request, promise, op, table, 0);
    try {
      promise.get();
    } catch (Exception e) {
      TableHandler.ReplicaConfiguration replicaConfig = table.getReplicaConfig(gpid.get_pidx());
      String server =
          replicaConfig.primaryAddress.get_ip() + ":" + replicaConfig.primaryAddress.get_port();

      String msg =
          String.format(
              "org.apache.pegasus.client.PException: {version}: org.apache.pegasus.rpc.ReplicationException: ERR_TIMEOUT: [metaServer=%s,table=temp,operation=put,request=%s,replicaServer=%s,gpid=(%s),timeout=1000ms] The operation is timed out!",
              client.getMetaList(), request.toString(), server, gpid.toString());
      assertEquals(e.getMessage(), msg);
    }
  }
}
