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

import static org.junit.jupiter.api.Assertions.*;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pegasus.apps.update_request;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.client.PegasusClient;
import org.apache.pegasus.operator.client_operator;
import org.apache.pegasus.operator.query_cfg_operator;
import org.apache.pegasus.operator.rrdb_get_operator;
import org.apache.pegasus.operator.rrdb_put_operator;
import org.apache.pegasus.replication.query_cfg_request;
import org.apache.pegasus.rpc.KeyHasher;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptorManager;
import org.apache.pegasus.tools.Toollet;
import org.apache.pegasus.tools.Tools;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class ReplicaSessionTest {
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSessionTest.class);
  private ClusterManager manager;

  @BeforeEach
  public void before(TestInfo testInfo) throws Exception {
    manager =
        new ClusterManager(
            ClientOptions.builder()
                .metaServers("127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603")
                .build());
    logger.info("test started: {}", testInfo.getDisplayName());
  }

  @AfterEach
  public void after() throws Exception {
    manager.close();
  }

  /** Method: connect() */
  @Test
  public void testConnect() throws Exception {
    // test1: connect to an invalid address.
    ReplicaSession rs =
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:12345")));

    ArrayList<FutureTask<Void>> callbacks = new ArrayList<>();

    for (int i = 0; i < 100; ++i) {
      final client_operator op = new rrdb_put_operator(new gpid(-1, -1), "", null, 0);
      final FutureTask<Void> cb =
          new FutureTask<>(
              () -> {
                assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                return null;
              });

      callbacks.add(cb);
      rs.asyncSend(op, cb, 1000, false);
    }

    for (FutureTask<Void> cb : callbacks) {
      try {
        Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
      } catch (ExecutionException e) {
        fail();
      }
    }

    final ReplicaSession finalRs = rs;
    assertTrue(
        Toollet.waitCondition(
            () -> ReplicaSession.ConnState.DISCONNECTED == finalRs.getState(), 5));

    // test2: connect to a valid address, and then close the server.
    callbacks.clear();

    final rpc_address addr = rpc_address.fromIpPort("127.0.0.1:34801");
    assertNotNull(addr);

    rs = manager.getReplicaSession(addr);
    rs.setMessageResponseFilter((err, header) -> true);
    for (int i = 0; i < 20; ++i) {
      // Send query request to replica server, expect it to be timeout.
      final update_request req =
          new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);
      final client_operator op = new Toollet.test_operator(new gpid(-1, -1), req);

      final int index = i;
      final FutureTask<Void> cb =
          new FutureTask<>(
              () -> {
                assertEquals(error_code.error_types.ERR_TIMEOUT, op.rpc_error.errno);
                // Kill the server at the last request.
                if (index == 19) {
                  Toollet.closeServer(addr);
                }
                return null;
              });

      callbacks.add(cb);
      rs.asyncSend(op, cb, 500, false);
    }

    for (int i = 0; i < 80; ++i) {
      // Re-send query request to replica server, but the timeout is longer.
      final update_request req =
          new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);
      final client_operator op = new Toollet.test_operator(new gpid(-1, -1), req);

      final FutureTask<Void> cb =
          new FutureTask<>(
              () -> {
                assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                return null;
              });

      callbacks.add(cb);

      // The request has longer timeout, so response should be received after the server was
      // killed.
      rs.asyncSend(op, cb, 2000, false);
    }

    for (FutureTask<Void> cb : callbacks) {
      try {
        Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
      } catch (ExecutionException e) {
        logger.error("failed while waiting for callback: ", e);
        fail();
      }
    }
    rs.setMessageResponseFilter(null);

    Toollet.tryStartServer(addr);
  }

  // ensure if response decode throws an exception, client is able to be informed.
  @Test
  public void testRecvInvalidData() throws Exception {
    class test_operator extends rrdb_get_operator {
      private test_operator(gpid gpid, blob request) {
        super(gpid, "", request, KeyHasher.DEFAULT.hash("a".getBytes()));
      }

      // should be called on ThriftFrameDecoder#decode
      @Override
      public void recv_data(TProtocol iprot) throws TException {
        throw new org.apache.thrift.TApplicationException(
            org.apache.thrift.TApplicationException.MISSING_RESULT, "get failed: unknown result");
      }
    }

    final ReplicaSession rs =
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34801")));

    for (int pid = 0; pid < 16; pid++) {
      // Find a valid partition held on 127.0.0.1:34801.
      blob req = new blob(PegasusClient.generateKey("a".getBytes(), "".getBytes()));
      final client_operator op = new test_operator(new gpid(1, pid), req);
      final FutureTask<Void> cb =
          new FutureTask<>(
              () -> {
                if (op.rpc_error.errno != error_code.error_types.ERR_OBJECT_NOT_FOUND
                    && op.rpc_error.errno != error_code.error_types.ERR_INVALID_STATE
                    && op.rpc_error.errno != error_code.error_types.ERR_SESSION_RESET) {
                  assertEquals(error_code.error_types.ERR_INVALID_DATA, op.rpc_error.errno);
                }
                return null;
              });
      rs.asyncSend(op, cb, 2000, false);
      Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
    }
  }

  @Test
  public void testTryNotifyWithSequenceID() throws Exception {
    final ReplicaSession rs =
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34801")));

    // There's no pending RequestEntry, ensure no NPE thrown.
    assertTrue(rs.pendingResponse.isEmpty());
    try {
      rs.tryNotifyFailureWithSeqID(100, error_code.error_types.ERR_TIMEOUT, false);
    } catch (Exception e) {
      assertNull(e);
    }

    // Edge case (this is not yet confirmed to happen)
    // seqId=100 in wait-queue, but entry.timeoutTask is set null because some sort of bug in netty.
    AtomicBoolean passed = new AtomicBoolean(false);
    ReplicaSession.RequestEntry entry = new ReplicaSession.RequestEntry();
    entry.sequenceId = 100;
    entry.callback = () -> passed.set(true);
    entry.timeoutTask = null; // Simulate the timeoutTask has been null.
    entry.op = new rrdb_put_operator(new gpid(1, 1), null, null, 0);
    rs.pendingResponse.put(100, entry);
    rs.tryNotifyFailureWithSeqID(100, error_code.error_types.ERR_TIMEOUT, false);
    assertTrue(passed.get());

    // Simulate the entry has been removed, ensure no NPE thrown.
    rs.getAndRemoveEntry(entry.sequenceId);
    rs.tryNotifyFailureWithSeqID(entry.sequenceId, entry.op.rpc_error.errno, true);

    // Ensure mark session state to disconnect when TryNotifyWithSequenceID incur any exception.
    final ReplicaSession mockRs = Mockito.spy(rs);
    mockRs.pendingSend.offer(entry);
    mockRs.fields.state = ReplicaSession.ConnState.CONNECTED;
    Mockito.doThrow(new Exception())
        .when(mockRs)
        .tryNotifyFailureWithSeqID(entry.sequenceId, entry.op.rpc_error.errno, false);
    mockRs.markSessionDisconnect();
    assertEquals(ReplicaSession.ConnState.DISCONNECTED, mockRs.getState());
  }

  @Test
  public void testCloseSession() throws InterruptedException {
    final ReplicaSession rs =
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34801")));
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(200);
    assertEquals(ReplicaSession.ConnState.CONNECTED, rs.getState());

    rs.closeSession();
    Thread.sleep(100);
    assertEquals(ReplicaSession.ConnState.DISCONNECTED, rs.getState());

    rs.fields.state = ReplicaSession.ConnState.CONNECTING;
    rs.closeSession();
    Thread.sleep(100);
    assertEquals(ReplicaSession.ConnState.DISCONNECTED, rs.getState());
  }

  @Test
  public void testSessionConnectTimeout() throws InterruptedException {
    // This website normally ignores incorrect request without replying.
    final rpc_address addr = rpc_address.fromIpPort("www.baidu.com:34801");

    final long start = System.currentTimeMillis();

    final EventLoopGroup rpcGroup = new NioEventLoopGroup(4);
    final EventLoopGroup timeoutTaskGroup = new NioEventLoopGroup(4);
    final ReplicaSession rs =
        new ReplicaSession(
            addr, rpcGroup, timeoutTaskGroup, 1000, 30, (ReplicaSessionInterceptorManager) null);
    rs.tryConnect().awaitUninterruptibly();

    final long end = System.currentTimeMillis();

    assertEquals((end - start) / 1000, 1); // ensure connect failed within 1sec
    Thread.sleep(100);
    assertEquals(ReplicaSession.ConnState.DISCONNECTED, rs.getState());
  }

  @Test
  public void testSessionTryConnectWhenConnected() throws InterruptedException {
    final ReplicaSession rs =
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34801")));
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(100);

    assertEquals(ReplicaSession.ConnState.CONNECTED, rs.getState());
    assertNull(rs.tryConnect()); // do not connect again
  }

  @Test
  public void testSessionAuth() throws InterruptedException, ExecutionException {
    // Connect to the meta server.
    final ReplicaSession rs =
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34601")));
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(100);
    assertEquals(ReplicaSession.ConnState.CONNECTED, rs.getState());

    // Send query_cfg_request to the meta server.
    final query_cfg_request queryCfgReq = new query_cfg_request("temp", new ArrayList<Integer>());
    final ReplicaSession.RequestEntry queryCfgEntry = new ReplicaSession.RequestEntry();
    queryCfgEntry.sequenceId = 100;
    queryCfgEntry.op = new query_cfg_operator(new gpid(-1, -1), queryCfgReq);
    final FutureTask<Void> cb =
        new FutureTask<>(
            () -> {
              // queryCfgReq should be sent successfully to the meta server.
              assertEquals(error_code.error_types.ERR_OK, queryCfgEntry.op.rpc_error.errno);
              return null;
            });
    queryCfgEntry.callback = cb;
    queryCfgEntry.timeoutTask = null;

    // Also insert into pendingResponse since ReplicaSession#sendPendingRequests() would check
    // sequenceId in it.
    assertTrue(rs.pendingResponse.isEmpty());
    rs.pendingResponse.put(queryCfgEntry.sequenceId, queryCfgEntry);

    // Initially session has not been authenticated, and queryCfgEntry(id=100) would be pending.
    assertTrue(rs.tryPendRequest(queryCfgEntry));

    // queryCfgEntry(id=100) should be the only element in the pending queue.
    rs.checkAuthPending(
        (Queue<ReplicaSession.RequestEntry> realAuthPendingSend) -> {
          assertEquals(1, realAuthPendingSend.size());
          ReplicaSession.RequestEntry entry = realAuthPendingSend.peek();
          assertNotNull(entry);
          assertEquals(100, entry.sequenceId);
          assertEquals(query_cfg_operator.class, entry.op.getClass());
        });

    // Authentication is successful, then the pending queryCfgEntry(id=100) should be sent.
    rs.onAuthSucceed();

    // Wait callback to be done.
    cb.get();

    // After the callback for auth success was called, nothing should be in the queue.
    rs.checkAuthPending(
        (Queue<ReplicaSession.RequestEntry> realAuthPendingSend) -> {
          assertTrue(realAuthPendingSend.isEmpty());
        });

    // tryPendRequest would return false at any time once authentication passed.
    queryCfgEntry.sequenceId = 101;
    assertFalse(rs.tryPendRequest(queryCfgEntry));

    // Now that authentication has passed, nothing should be in the queue.
    rs.checkAuthPending(
        (Queue<ReplicaSession.RequestEntry> realAuthPendingSend) -> {
          assertTrue(realAuthPendingSend.isEmpty());
        });

    // The session should keep connected before it is closed.
    assertEquals(ReplicaSession.ConnState.CONNECTED, rs.getState());
    rs.closeSession();
    Thread.sleep(100);
    assertEquals(ReplicaSession.ConnState.DISCONNECTED, rs.getState());

    // Authentication would be reset after the session is closed.
    queryCfgEntry.sequenceId = 102;
    assertTrue(rs.tryPendRequest(queryCfgEntry));

    // Clear the pending queue.
    rs.checkAuthPending(Collection::clear);
  }
}
