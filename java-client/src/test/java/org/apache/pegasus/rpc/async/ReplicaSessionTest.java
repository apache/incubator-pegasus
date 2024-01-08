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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.ArrayList;
import java.util.concurrent.Callable;
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
import org.apache.pegasus.operator.rrdb_get_operator;
import org.apache.pegasus.operator.rrdb_put_operator;
import org.apache.pegasus.rpc.KeyHasher;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptorManager;
import org.apache.pegasus.tools.Toollet;
import org.apache.pegasus.tools.Tools;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class ReplicaSessionTest {
  private String metaList = "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSessionTest.class);
  private ClusterManager manager;

  @BeforeEach
  public void before() throws Exception {
    manager = new ClusterManager(ClientOptions.builder().metaServers(metaList).build());
  }

  @AfterEach
  public void after() throws Exception {
    manager.close();
  }

  /** Method: connect() */
  @Test
  public void testConnect() throws Exception {
    // test1: connect to an invalid address.
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:12345");
    ReplicaSession rs = manager.getReplicaSession(addr);

    ArrayList<FutureTask<Void>> callbacks = new ArrayList<FutureTask<Void>>();

    for (int i = 0; i < 100; ++i) {
      final client_operator op = new rrdb_put_operator(new gpid(-1, -1), "", null, 0);
      final FutureTask<Void> cb =
          new FutureTask<Void>(
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

    final ReplicaSession cp_rs = rs;
    Toollet.waitCondition(() -> ReplicaSession.ConnState.DISCONNECTED == cp_rs.getState(), 5);

    // test2: connect to a valid address, and then close the server.
    addr.fromString("127.0.0.1:34801");
    callbacks.clear();

    rs = manager.getReplicaSession(addr);
    rs.setMessageResponseFilter((err, header) -> true);
    for (int i = 0; i < 20; ++i) {
      // Send query request to replica server, expect it to be timeout.
      final int index = i;
      update_request req =
          new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);

      final client_operator op = new Toollet.test_operator(new gpid(-1, -1), req);
      final rpc_address cp_addr = addr;
      final FutureTask<Void> cb =
          new FutureTask<Void>(
              () -> {
                assertEquals(error_code.error_types.ERR_TIMEOUT, op.rpc_error.errno);
                // for the last request, we kill the server
                if (index == 19) {
                  Toollet.closeServer(cp_addr);
                }
                return null;
              });

      callbacks.add(cb);
      rs.asyncSend(op, cb, 500, false);
    }

    for (int i = 0; i < 80; ++i) {
      // Re-send query request to replica server, but the timeout is longer.
      update_request req =
          new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);
      final client_operator op = new Toollet.test_operator(new gpid(-1, -1), req);
      final FutureTask<Void> cb =
          new FutureTask<Void>(
              () -> {
                assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                return null;
              });

      callbacks.add(cb);
      // The request has longer timeout, so it should be responsed later than the server been
      // killed.
      rs.asyncSend(op, cb, 2000, false);
    }

    for (FutureTask<Void> cb : callbacks) {
      try {
        Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
      } catch (ExecutionException e) {
        e.printStackTrace();
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

    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);

    for (int pid = 0; pid < 16; pid++) {
      // find a valid partition held on 127.0.0.1:34801
      blob req = new blob(PegasusClient.generateKey("a".getBytes(), "".getBytes()));
      final client_operator op = new test_operator(new gpid(1, pid), req);
      FutureTask<Void> cb =
          new FutureTask<Void>(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  if (op.rpc_error.errno != error_code.error_types.ERR_OBJECT_NOT_FOUND
                      && op.rpc_error.errno != error_code.error_types.ERR_INVALID_STATE
                      && op.rpc_error.errno != error_code.error_types.ERR_SESSION_RESET) {
                    assertEquals(error_code.error_types.ERR_INVALID_DATA, op.rpc_error.errno);
                  }
                  return null;
                }
              });
      rs.asyncSend(op, cb, 2000, false);
      Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
    }
  }

  @Test
  public void testTryNotifyWithSequenceID() throws Exception {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);

    // no pending RequestEntry, ensure no NPE thrown
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
    entry.timeoutTask = null; // simulate the timeoutTask has been null
    entry.op = new rrdb_put_operator(new gpid(1, 1), null, null, 0);
    rs.pendingResponse.put(100, entry);
    rs.tryNotifyFailureWithSeqID(100, error_code.error_types.ERR_TIMEOUT, false);
    assertTrue(passed.get());

    // simulate the entry has been removed, ensure no NPE thrown
    rs.getAndRemoveEntry(entry.sequenceId);
    rs.tryNotifyFailureWithSeqID(entry.sequenceId, entry.op.rpc_error.errno, true);

    // ensure mark session state to disconnect when TryNotifyWithSequenceID incur any exception
    ReplicaSession mockRs = Mockito.spy(rs);
    mockRs.pendingSend.offer(entry);
    mockRs.fields.state = ReplicaSession.ConnState.CONNECTED;
    Mockito.doThrow(new Exception())
        .when(mockRs)
        .tryNotifyFailureWithSeqID(entry.sequenceId, entry.op.rpc_error.errno, false);
    mockRs.markSessionDisconnect();
    assertEquals(mockRs.getState(), ReplicaSession.ConnState.DISCONNECTED);
  }

  @Test
  public void testCloseSession() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(200);
    assertEquals(rs.getState(), ReplicaSession.ConnState.CONNECTED);

    rs.closeSession();
    Thread.sleep(100);
    assertEquals(rs.getState(), ReplicaSession.ConnState.DISCONNECTED);

    rs.fields.state = ReplicaSession.ConnState.CONNECTING;
    rs.closeSession();
    Thread.sleep(100);
    assertEquals(rs.getState(), ReplicaSession.ConnState.DISCONNECTED);
  }

  @Test
  public void testSessionConnectTimeout() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString(
        "www.baidu.com:34801"); // this website normally ignores incorrect request without replying

    long start = System.currentTimeMillis();
    EventLoopGroup rpcGroup = new NioEventLoopGroup(4);
    EventLoopGroup timeoutTaskGroup = new NioEventLoopGroup(4);
    ReplicaSession rs =
        new ReplicaSession(
            addr, rpcGroup, timeoutTaskGroup, 1000, 30, (ReplicaSessionInterceptorManager) null);
    rs.tryConnect().awaitUninterruptibly();
    long end = System.currentTimeMillis();
    assertEquals((end - start) / 1000, 1); // ensure connect failed within 1sec
    Thread.sleep(100);
    assertEquals(rs.getState(), ReplicaSession.ConnState.DISCONNECTED);
  }

  @Test
  public void testSessionTryConnectWhenConnected() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(100);
    assertEquals(rs.getState(), ReplicaSession.ConnState.CONNECTED);
    assertNull(rs.tryConnect()); // do not connect again
  }
}
