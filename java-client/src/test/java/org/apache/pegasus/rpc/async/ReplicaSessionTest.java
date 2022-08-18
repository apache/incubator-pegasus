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
import org.apache.pegasus.operator.ClientOperator;
import org.apache.pegasus.operator.RRDBGetOperator;
import org.apache.pegasus.operator.RRDBPutOperator;
import org.apache.pegasus.rpc.KeyHasher;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptorManager;
import org.apache.pegasus.tools.Toollet;
import org.apache.pegasus.tools.Tools;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class ReplicaSessionTest {
  private String metaList = "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSessionTest.class);
  private ClusterManager manager;

  @Before
  public void before() throws Exception {
    manager = new ClusterManager(ClientOptions.builder().metaServers(metaList).build());
  }

  @After
  public void after() throws Exception {
    manager.close();
  }

  /** Method: connect() */
  @Test
  public void testConnect() throws Exception {
    // test1: connect to a invalid address
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:12345");
    ReplicaSession rs = manager.getReplicaSession(addr);

    ArrayList<FutureTask<Void>> callbacks = new ArrayList<FutureTask<Void>>();

    for (int i = 0; i < 100; ++i) {
      final ClientOperator op = new RRDBPutOperator(new gpid(-1, -1), "", null, 0);
      final FutureTask<Void> cb =
          new FutureTask<Void>(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  Assert.assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                  return null;
                }
              });

      callbacks.add(cb);
      rs.asyncSend(op, cb, 1000, false);
    }

    for (FutureTask<Void> cb : callbacks) {
      try {
        Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
      } catch (ExecutionException e) {
        Assert.fail();
      }
    }

    final ReplicaSession cp_rs = rs;
    Toollet.waitCondition(
        new Toollet.BoolCallable() {
          @Override
          public boolean call() {
            return ReplicaSession.ConnState.DISCONNECTED == cp_rs.getState();
          }
        },
        5);

    // test2: connect to an valid address, and then close the server
    addr.fromString("127.0.0.1:34801");
    callbacks.clear();

    rs = manager.getReplicaSession(addr);
    rs.setMessageResponseFilter(
        new ReplicaSession.MessageResponseFilter() {
          @Override
          public boolean abandonIt(error_code.error_types err, TMessage header) {
            return true;
          }
        });
    for (int i = 0; i < 20; ++i) {
      // we send query request to replica server. We expect it to discard it.
      final int index = i;
      update_request req =
          new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);

      final ClientOperator op = new Toollet.test_operator(new gpid(-1, -1), req);
      final rpc_address cp_addr = addr;
      final FutureTask<Void> cb =
          new FutureTask<Void>(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  Assert.assertEquals(error_code.error_types.ERR_TIMEOUT, op.rpc_error.errno);
                  // for the last request, we kill the server
                  if (index == 19) {
                    Toollet.closeServer(cp_addr);
                  }
                  return null;
                }
              });

      callbacks.add(cb);
      rs.asyncSend(op, cb, 500, false);
    }

    for (int i = 0; i < 80; ++i) {
      // then we still send query request to replica server. But the timeout is longer.
      update_request req =
          new update_request(new blob("hello".getBytes()), new blob("world".getBytes()), 0);
      final ClientOperator op = new Toollet.test_operator(new gpid(-1, -1), req);
      final FutureTask<Void> cb =
          new FutureTask<Void>(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  Assert.assertEquals(error_code.error_types.ERR_SESSION_RESET, op.rpc_error.errno);
                  return null;
                }
              });

      callbacks.add(cb);
      // these requests have longer timeout, so they should be responsed later than the server is
      // killed
      rs.asyncSend(op, cb, 2000, false);
    }

    for (FutureTask<Void> cb : callbacks) {
      try {
        Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
      } catch (ExecutionException e) {
        e.printStackTrace();
        Assert.fail();
      }
    }
    rs.setMessageResponseFilter(null);

    Toollet.tryStartServer(addr);
  }

  // ensure if response decode throws an exception, client is able to be informed.
  @Test
  public void testRecvInvalidData() throws Exception {
    class test_operator extends RRDBGetOperator {
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
      final ClientOperator op = new test_operator(new gpid(1, pid), req);
      FutureTask<Void> cb =
          new FutureTask<Void>(
              new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                  if (op.rpc_error.errno != error_code.error_types.ERR_OBJECT_NOT_FOUND
                      && op.rpc_error.errno != error_code.error_types.ERR_INVALID_STATE
                      && op.rpc_error.errno != error_code.error_types.ERR_SESSION_RESET) {
                    Assert.assertEquals(
                        error_code.error_types.ERR_INVALID_DATA, op.rpc_error.errno);
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
    Assert.assertTrue(rs.pendingResponse.isEmpty());
    try {
      rs.tryNotifyFailureWithSeqID(100, error_code.error_types.ERR_TIMEOUT, false);
    } catch (Exception e) {
      Assert.assertNull(e);
    }

    // Edge case (this is not yet confirmed to happen)
    // seqId=100 in wait-queue, but entry.timeoutTask is set null because some sort of bug in netty.
    AtomicBoolean passed = new AtomicBoolean(false);
    ReplicaSession.RequestEntry entry = new ReplicaSession.RequestEntry();
    entry.sequenceId = 100;
    entry.callback = () -> passed.set(true);
    entry.timeoutTask = null; // simulate the timeoutTask has been null
    entry.op = new RRDBPutOperator(new gpid(1, 1), null, null, 0);
    rs.pendingResponse.put(100, entry);
    rs.tryNotifyFailureWithSeqID(100, error_code.error_types.ERR_TIMEOUT, false);
    Assert.assertTrue(passed.get());

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
    Assert.assertEquals(mockRs.getState(), ReplicaSession.ConnState.DISCONNECTED);
  }

  @Test
  public void testCloseSession() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(200);
    Assert.assertEquals(rs.getState(), ReplicaSession.ConnState.CONNECTED);

    rs.closeSession();
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ReplicaSession.ConnState.DISCONNECTED);

    rs.fields.state = ReplicaSession.ConnState.CONNECTING;
    rs.closeSession();
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ReplicaSession.ConnState.DISCONNECTED);
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
    Assert.assertEquals((end - start) / 1000, 1); // ensure connect failed within 1sec
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ReplicaSession.ConnState.DISCONNECTED);
  }

  @Test
  public void testSessionTryConnectWhenConnected() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ReplicaSession.ConnState.CONNECTED);
    Assert.assertNull(rs.tryConnect()); // do not connect again
  }
}
