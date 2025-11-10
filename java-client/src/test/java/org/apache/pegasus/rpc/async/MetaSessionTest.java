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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.operator.client_operator;
import org.apache.pegasus.operator.query_cfg_operator;
import org.apache.pegasus.replication.partition_configuration;
import org.apache.pegasus.replication.query_cfg_request;
import org.apache.pegasus.replication.query_cfg_response;
import org.apache.pegasus.tools.Toollet;
import org.apache.pegasus.tools.Tools;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;

public class MetaSessionTest {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MetaSessionTest.class);

  // "Mockito.when(meta.resolve(("localhost:34601"))).thenReturn(addrs)" is for simulating DNS
  // resolution: <localhost:34601>-><addrs>

  @BeforeEach
  public void before() throws Exception {}

  @AfterEach
  public void after() throws Exception {
    Toollet.tryStartServer(Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34602")));
  }

  private static void ensureNotLeader(rpc_address addr) {
    Toollet.closeServer(addr);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      logger.error("failed while sleeping: ", e);
    }
    Toollet.tryStartServer(addr);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      logger.error("failed while sleeping: ", e);
    }
  }

  /** Method: connect() */
  @Test
  public void testMetaConnect() throws Exception {
    // test: first connect to a wrong server
    // then it forward to the right server
    // then the wrong server crashed
    String address_list = "127.0.0.1:34602,127.0.0.1:34603,127.0.0.1:34601";
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers(address_list).build());
    MetaSession session = manager.getMetaSession();

    rpc_address addr = Objects.requireNonNull(rpc_address.fromIpPort("127.0.0.1:34602"));
    ensureNotLeader(addr);

    ArrayList<FutureTask<Void>> callbacks = new ArrayList<>();
    for (int i = 0; i < 1000; ++i) {
      query_cfg_request req = new query_cfg_request("temp", new ArrayList<Integer>());
      final client_operator op = new query_cfg_operator(new gpid(-1, -1), req);
      FutureTask<Void> callback =
          new FutureTask<>(
              () -> {
                assertEquals(error_code.error_types.ERR_OK, op.rpc_error.errno);
                return null;
              });
      callbacks.add(callback);
      session.asyncExecute(op, callback, 10);
    }

    Toollet.closeServer(addr);
    for (FutureTask<Void> cb : callbacks) {
      try {
        Tools.waitUninterruptable(cb, Integer.MAX_VALUE);
      } catch (ExecutionException e) {
        logger.error("failed while waiting for callback", e);
        fail();
      }
    }

    manager.close();
  }

  private rpc_address[] getAddressFromSession(List<ReplicaSession> sessions) {
    rpc_address[] results = new rpc_address[sessions.size()];
    for (int i = 0; i < results.length; i++) {
      results[i] = sessions.get(i).getAddress();
    }
    return results;
  }

  @Test
  public void testDNSResolveHost() throws Exception {
    // ensure meta list keeps consistent with dns.

    String address_list = "127.0.0.1:34602, 127.0.0.1:34603, 127.0.0.1:34601";
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers(address_list).build());
    MetaSession session = manager.getMetaSession();
    MetaSession meta = Mockito.spy(session);
    ReplicaSession meta2 = meta.getMetaList().get(0); // 127.0.0.1:34602
    meta2.tryConnect();
    while (meta2.getState() != ReplicaSession.ConnState.CONNECTED) {
      Thread.sleep(1);
    }
    assertEquals(meta2.getState(), ReplicaSession.ConnState.CONNECTED);

    // DNS refreshed
    rpc_address[] addrs = new rpc_address[2];
    addrs[0] = rpc_address.fromIpPort("172.0.0.1:34601");
    addrs[1] = rpc_address.fromIpPort("172.0.0.2:34601");
    // simulating DNS resolution:localhost:34601->{172.0.0.1:34601,172.0.0.2:34601}
    Mockito.when(meta.resolve(("localhost:34601"))).thenReturn(addrs);
    assertArrayEquals(meta.resolve("localhost:34601"), addrs);
    meta.resolveHost("localhost:34601"); // update local meta list
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs);
    while (meta2.getState() != ReplicaSession.ConnState.DISCONNECTED) {
      Thread.sleep(1);
    }
    // ensure MetaSession#resolveHost will close unused sessions.
    assertEquals(meta2.getState(), ReplicaSession.ConnState.DISCONNECTED);

    // DNS refreshed again
    addrs = new rpc_address[2];
    addrs[0] = rpc_address.fromIpPort("172.0.0.1:34601");
    addrs[1] = rpc_address.fromIpPort("172.0.0.3:34601");
    // simulating DNS resolution:localhost:34601->{172.0.0.1:34601,172.0.0.3:34601}
    Mockito.when(meta.resolve(("localhost:34601"))).thenReturn(addrs);
    meta.resolveHost("localhost:34601");
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs);

    manager.close();
  }

  @Test
  public void testDNSMetaAllChanged() throws Exception {
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers("localhost:34601").build());
    MetaSession session = manager.getMetaSession();
    MetaSession meta = Mockito.spy(session);
    // curLeader=0, hostPort="localhost:34601"

    // metaList = 172.0.0.1:34601, 172.0.0.2:34601
    rpc_address[] addrs = new rpc_address[2];
    addrs[0] = rpc_address.fromIpPort("172.0.0.1:34601");
    addrs[1] = rpc_address.fromIpPort("172.0.0.2:34601");
    // simulating DNS resolution:localhost:34601->{172.0.0.1:34601,172.0.0.2:34601}
    Mockito.when(meta.resolve(("localhost:34601"))).thenReturn(addrs);
    meta.resolveHost("localhost:34601");
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs);

    query_cfg_request req = new query_cfg_request("temp", new ArrayList<Integer>());
    client_operator op = new query_cfg_operator(new gpid(-1, -1), req);
    op.rpc_error.errno = error_code.error_types.ERR_SESSION_RESET;
    MetaSession.MetaRequestRound round =
        new MetaSession.MetaRequestRound(
            op,
            new Runnable() {
              @Override
              public void run() {}
            },
            10,
            meta.getMetaList().get(0));

    // simulate a failed query meta, but ensure it will not retry after a failure.
    Mockito.doNothing().when(meta).retryQueryMeta(round, false);

    // DNS updated.
    rpc_address[] addrs2 = new rpc_address[2];
    addrs2[0] = rpc_address.fromIpPort("172.0.0.3:34601");
    addrs2[1] = rpc_address.fromIpPort("172.0.0.4:34601");
    // simulating DNS resolution:localhost:34601->{172.0.0.3:34601,172.0.0.4:34601}
    Mockito.when(meta.resolve(("localhost:34601"))).thenReturn(addrs2);

    // meta all dead, query failed.
    meta.onFinishQueryMeta(round);
    // switch curLeader to 1, meta list unchanged.
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs);
    Integer curLeader = (Integer) FieldUtils.readField(meta, "curLeader", true);
    assertEquals(curLeader.intValue(), 1);

    // failed again
    meta.onFinishQueryMeta(round);
    // switch curLeader to 0, meta list updated
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs2);
    curLeader = (Integer) FieldUtils.readField(meta, "curLeader", true);
    assertEquals(curLeader.intValue(), 0);

    // retry
    meta.onFinishQueryMeta(round);
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs2);
  }

  @Test
  public void testMetaForwardUnknownPrimary() throws Exception {
    // ensures that client will accept the forwarded meta
    // into local meta list, and set it to current leader.

    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers("localhost:34601").build());
    MetaSession session = manager.getMetaSession();
    MetaSession meta = Mockito.spy(session);
    // curLeader=0, hostPort="localhost:34601"

    // metaList = 172.0.0.1:34601, 172.0.0.2:34601
    rpc_address[] addrs = new rpc_address[2];
    addrs[0] = rpc_address.fromIpPort("172.0.0.1:34601");
    addrs[1] = rpc_address.fromIpPort("172.0.0.2:34601");
    Mockito.when(meta.resolve(("localhost:34601"))).thenReturn(addrs);
    meta.resolveHost("localhost:34601");
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs);

    query_cfg_request req = new query_cfg_request("temp", new ArrayList<Integer>());
    query_cfg_operator op = new query_cfg_operator(new gpid(-1, -1), req);
    op.rpc_error.errno = error_code.error_types.ERR_OK;
    FieldUtils.writeField(op, "response", new query_cfg_response(), true);
    op.get_response().err = new error_code();
    op.get_response().err.errno = error_code.error_types.ERR_FORWARD_TO_OTHERS;
    op.get_response().partitions = Collections.singletonList(new partition_configuration());
    op.get_response().partitions.get(0).primary = rpc_address.fromIpPort("172.0.0.3:34601");
    MetaSession.MetaRequestRound round =
        new MetaSession.MetaRequestRound(op, () -> {}, 10, meta.getMetaList().get(0));

    // do not retry after a failed QueryMeta.
    Mockito.doNothing().when(meta).retryQueryMeta(round, false);

    // failed to query meta
    meta.onFinishQueryMeta(round);

    rpc_address[] addrs2 = Arrays.copyOf(addrs, 3);
    addrs2[2] = rpc_address.fromIpPort("172.0.0.3:34601");

    // forward to 172.0.0.3:34601
    assertArrayEquals(getAddressFromSession(meta.getMetaList()), addrs2);
    Integer curLeader = (Integer) FieldUtils.readField(meta, "curLeader", true);
    assertEquals(curLeader.intValue(), 2);
  }

  @Test
  public void testDNSResetMetaMaxQueryCount() {
    ClusterManager manager =
        new ClusterManager(ClientOptions.builder().metaServers("localhost:34601").build());
    MetaSession metaMock = Mockito.spy(manager.getMetaSession());

    List<ReplicaSession> metaList = metaMock.getMetaList();
    metaList.remove(0); // del the "localhost:34601"
    metaList.add(
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("172.0.0.1:34602"))));
    metaList.add(
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("172.0.0.1:34603"))));
    metaList.add(
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("172.0.0.1:34601"))));

    rpc_address[] newAddrs = new rpc_address[5];
    newAddrs[0] = rpc_address.fromIpPort("137.0.0.1:34602");
    newAddrs[1] = rpc_address.fromIpPort("137.0.0.1:34603");
    // one of the followings is the real primary.
    newAddrs[2] = rpc_address.fromIpPort("127.0.0.1:34602");
    newAddrs[3] = rpc_address.fromIpPort("127.0.0.1:34603");
    newAddrs[4] = rpc_address.fromIpPort("127.0.0.1:34601");

    // DNS refreshed
    Mockito.when(metaMock.resolve("localhost:34601")).thenReturn(newAddrs);

    query_cfg_request req = new query_cfg_request("temp", new ArrayList<Integer>());
    client_operator op = new query_cfg_operator(new gpid(-1, -1), req);

    // `MetaSession#query` will first query the 3 old addresses (and failed), then resolve the DNS
    // and find the 5 new addresses.
    // Even though the given maxQueryCount is given 3, the total query count is at least 6.
    metaMock.execute(op, metaList.size());
    error_code.error_types err = MetaSession.getMetaServiceError(op);
    assertEquals(error_code.error_types.ERR_OK, err);
  }

  @Test
  public void testDNSMetaUnavailable() {
    // Ensures when the DNS returns meta all unavailable, finally the query will timeout.
    ClusterManager manager =
        new ClusterManager(
            ClientOptions.builder()
                .metaServers("localhost:34601")
                .metaQueryTimeout(Duration.ofMillis(1000))
                .build());
    MetaSession metaMock = Mockito.spy(manager.getMetaSession());
    List<ReplicaSession> metaList = metaMock.getMetaList();
    metaList.clear(); // del the "localhost:34601" resolve right results
    metaList.add(
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("172.0.0.1:34602"))));
    metaList.add(
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("172.0.0.1:34603"))));
    metaList.add(
        manager.getReplicaSession(
            Objects.requireNonNull(rpc_address.fromIpPort("172.0.0.1:34601"))));
    rpc_address[] newAddrs =
        new rpc_address[] {
          rpc_address.fromIpPort("137.0.0.1:34602"),
          rpc_address.fromIpPort("137.0.0.1:34603"),
          rpc_address.fromIpPort("137.0.0.1:34601")
        };
    Mockito.when(metaMock.resolve("localhost:34601")).thenReturn(newAddrs);
    query_cfg_request req = new query_cfg_request("temp", new ArrayList<Integer>());
    client_operator op = new query_cfg_operator(new gpid(-1, -1), req);
    metaMock.execute(op, metaList.size());
    assertEquals(error_code.error_types.ERR_TIMEOUT, MetaSession.getMetaServiceError(op));
  }
}
