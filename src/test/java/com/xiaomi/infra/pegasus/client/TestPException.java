// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package com.xiaomi.infra.pegasus.client;

import com.xiaomi.infra.pegasus.apps.update_request;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.base.error_code.error_types;
import com.xiaomi.infra.pegasus.base.gpid;
import com.xiaomi.infra.pegasus.operator.rrdb_put_operator;
import com.xiaomi.infra.pegasus.rpc.KeyHasher;
import com.xiaomi.infra.pegasus.rpc.async.ClusterManager;
import com.xiaomi.infra.pegasus.rpc.async.TableHandler;
import io.netty.util.concurrent.DefaultPromise;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Test;

public class TestPException {
  @Test
  public void testThreadInterrupted() throws Exception {
    PException ex = PException.threadInterrupted("test", new InterruptedException("intxxx"));
    Assert.assertEquals(
        "{version}: com.xiaomi.infra.pegasus.rpc.ReplicationException: ERR_THREAD_INTERRUPTED: [table=test] Thread was interrupted: intxxx",
        ex.getMessage());
  }

  @Test
  public void testTimeout() throws Exception {
    PException ex = PException.timeout("test", 1000, new TimeoutException("tmxxx"));
    Assert.assertEquals(
        "{version}: com.xiaomi.infra.pegasus.rpc.ReplicationException: ERR_TIMEOUT: [table=test, timeout=1000ms] Timeout on Future await: tmxxx",
        ex.getMessage());
  }

  @Test
  public void testVersion() {
    // Test the constructors of PException

    PException ex = new PException("test");
    Assert.assertEquals("{version}: test", ex.getMessage());

    ex = new PException("test", new TimeoutException());
    Assert.assertEquals("{version}: test", ex.getMessage());
  }

  @Test
  public void testHandleReplicationException() throws Exception {
    String[] metaList = {"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"};
    ClusterManager manager = new ClusterManager(1000, 1, false, null, 60, metaList);
    TableHandler table = manager.openTable("temp", KeyHasher.DEFAULT);
    DefaultPromise<Void> promise = table.newPromise();
    update_request req = new update_request(new blob(), new blob(), 100);
    gpid gpid = table.getGpidByHash(1);
    rrdb_put_operator op = new rrdb_put_operator(gpid, table.getTableName(), req, 0);
    op.rpc_error.errno = error_code.error_types.ERR_OBJECT_NOT_FOUND;

    // set failure in promise, the exception is thrown as ExecutionException.
    PegasusTable pegasusTable = new PegasusTable(null, table);
    pegasusTable.handleReplicaException(promise, op, table, 1000);
    try {
      promise.get();
    } catch (ExecutionException e) {
      TableHandler.ReplicaConfiguration replicaConfig = table.getReplicaConfig(gpid.get_pidx());
      String server = replicaConfig.primary.get_ip() + ":" + replicaConfig.primary.get_port();

      String msg =
          String.format(
              "com.xiaomi.infra.pegasus.client.PException: {version}: com.xiaomi.infra.pegasus.rpc.ReplicationException: ERR_OBJECT_NOT_FOUND: [table=temp,operation=put,replicaServer=%s,gpid=(%s)] The replica server doesn't serve this partition!",
              server, gpid.toString());
      Assert.assertEquals(e.getMessage(), msg);
      return;
    } catch (InterruptedException e) {
      Assert.fail();
    }
    Assert.fail();
  }

  @Test
  public void testTimeOutIsZero() throws Exception {
    // ensure "PException ERR_TIMEOUT" is thrown with the real timeout value, when user given
    // timeout is 0.
    String[] metaList = {"127.0.0.1:34601", "127.0.0.1:34602", "127.0.0.1:34603"};
    ClusterManager manager = new ClusterManager(1000, 1, false, null, 60, metaList);
    TableHandler table = manager.openTable("temp", KeyHasher.DEFAULT);
    DefaultPromise<Void> promise = table.newPromise();
    update_request req = new update_request(new blob(), new blob(), 100);
    gpid gpid = table.getGpidByHash(1);
    rrdb_put_operator op = new rrdb_put_operator(gpid, table.getTableName(), req, 0);
    op.rpc_error.errno = error_types.ERR_TIMEOUT;

    PegasusTable pegasusTable = new PegasusTable(null, table);
    pegasusTable.handleReplicaException(promise, op, table, 0);
    try {
      promise.get();
    } catch (Exception e) {
      TableHandler.ReplicaConfiguration replicaConfig = table.getReplicaConfig(gpid.get_pidx());
      String server = replicaConfig.primary.get_ip() + ":" + replicaConfig.primary.get_port();

      String msg =
          String.format(
              "com.xiaomi.infra.pegasus.client.PException: {version}: com.xiaomi.infra.pegasus.rpc.ReplicationException: ERR_TIMEOUT: [table=temp,operation=put,replicaServer=%s,gpid=(%s)] The operation timeout is 1000ms!",
              server, gpid.toString());
      Assert.assertEquals(e.getMessage(), msg);
    }
  }
}
