// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.apps.update_request;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.base.gpid;
import com.xiaomi.infra.pegasus.base.rpc_address;
import com.xiaomi.infra.pegasus.client.PegasusClient;
import com.xiaomi.infra.pegasus.operator.client_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_get_operator;
import com.xiaomi.infra.pegasus.operator.rrdb_put_operator;
import com.xiaomi.infra.pegasus.rpc.KeyHasher;
import com.xiaomi.infra.pegasus.rpc.async.ReplicaSession.ConnState;
import com.xiaomi.infra.pegasus.thrift.TException;
import com.xiaomi.infra.pegasus.thrift.protocol.TMessage;
import com.xiaomi.infra.pegasus.thrift.protocol.TProtocol;
import com.xiaomi.infra.pegasus.tools.Toollet;
import com.xiaomi.infra.pegasus.tools.Tools;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class ReplicaSessionTest {
  private String[] metaList = {"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"};
  private final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSessionTest.class);
  private ClusterManager manager;

  @Before
  public void before() throws Exception {
    manager = new ClusterManager(1000, 1, false, null, 60, metaList);
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
      final client_operator op =
          new rrdb_put_operator(new com.xiaomi.infra.pegasus.base.gpid(-1, -1), "", null, 0);
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

      final client_operator op =
          new Toollet.test_operator(new com.xiaomi.infra.pegasus.base.gpid(-1, -1), req);
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
      final client_operator op =
          new Toollet.test_operator(new com.xiaomi.infra.pegasus.base.gpid(-1, -1), req);
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
    class test_operator extends rrdb_get_operator {
      private test_operator(gpid gpid, blob request) {
        super(gpid, "", request, KeyHasher.DEFAULT.hash("a".getBytes()));
      }

      // should be called on ThriftFrameDecoder#decode
      @Override
      public void recv_data(TProtocol iprot) throws TException {
        throw new com.xiaomi.infra.pegasus.thrift.TApplicationException(
            com.xiaomi.infra.pegasus.thrift.TApplicationException.MISSING_RESULT,
            "get failed: unknown result");
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
    entry.op = new rrdb_put_operator(new gpid(1, 1), null, null, 0);
    rs.pendingResponse.put(100, entry);
    rs.tryNotifyFailureWithSeqID(100, error_code.error_types.ERR_TIMEOUT, false);
    Assert.assertTrue(passed.get());

    // simulate the entry has been removed, ensure no NPE thrown
    rs.getAndRemoveEntry(entry.sequenceId);
    rs.tryNotifyFailureWithSeqID(entry.sequenceId, entry.op.rpc_error.errno, true);

    // ensure mark session state to disconnect when TryNotifyWithSequenceID incur any exception
    ReplicaSession mockRs = Mockito.spy(rs);
    mockRs.pendingSend.offer(entry);
    mockRs.fields.state = ConnState.CONNECTED;
    Mockito.doThrow(new Exception())
        .when(mockRs)
        .tryNotifyFailureWithSeqID(entry.sequenceId, entry.op.rpc_error.errno, false);
    mockRs.markSessionDisconnect();
    Assert.assertEquals(mockRs.getState(), ConnState.DISCONNECTED);
  }

  @Test
  public void testCloseSession() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(200);
    Assert.assertEquals(rs.getState(), ConnState.CONNECTED);

    rs.closeSession();
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ConnState.DISCONNECTED);

    rs.fields.state = ConnState.CONNECTING;
    rs.closeSession();
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ConnState.DISCONNECTED);
  }

  @Test
  public void testSessionConnectTimeout() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString(
        "www.baidu.com:34801"); // this website normally ignores incorrect request without replying

    long start = System.currentTimeMillis();
    EventLoopGroup rpcGroup = new NioEventLoopGroup(4);
    ReplicaSession rs = new ReplicaSession(addr, rpcGroup, 1000);
    rs.tryConnect().awaitUninterruptibly();
    long end = System.currentTimeMillis();
    Assert.assertEquals((end - start) / 1000, 1); // ensure connect failed within 1sec
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ConnState.DISCONNECTED);
  }

  @Test
  public void testSessionTryConnectWhenConnected() throws InterruptedException {
    rpc_address addr = new rpc_address();
    addr.fromString("127.0.0.1:34801");
    ReplicaSession rs = manager.getReplicaSession(addr);
    rs.tryConnect().awaitUninterruptibly();
    Thread.sleep(100);
    Assert.assertEquals(rs.getState(), ConnState.CONNECTED);
    Assert.assertNull(rs.tryConnect()); // do not connect again
  }
}
