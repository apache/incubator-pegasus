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

import io.netty.channel.ChannelFuture;
import io.netty.util.concurrent.EventExecutor;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.client.FutureGroup;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.retry.RetryPolicy;
import org.apache.pegasus.operator.client_operator;
import org.apache.pegasus.operator.query_cfg_operator;
import org.apache.pegasus.replication.partition_configuration;
import org.apache.pegasus.replication.query_cfg_request;
import org.apache.pegasus.replication.query_cfg_response;
import org.apache.pegasus.rpc.InternalTableOptions;
import org.apache.pegasus.rpc.ReplicationException;
import org.apache.pegasus.rpc.Table;
import org.apache.pegasus.rpc.interceptor.TableInterceptorManager;
import org.slf4j.Logger;

public class TableHandler extends Table {
  public static final class ReplicaConfiguration {
    public gpid pid = new gpid();
    public long ballot = 0;
    public rpc_address primaryAddress = new rpc_address();
    public ReplicaSession primarySession = null;
    public List<ReplicaSession> secondarySessions = new ArrayList<>();
  }

  static final class TableConfiguration {
    ArrayList<ReplicaConfiguration> replicas;
    long updateVersion;
  }

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(TableHandler.class);
  ClusterManager manager_;
  EventExecutor executor_; // should be only one thread in this service

  AtomicReference<TableConfiguration> tableConfig_;
  AtomicBoolean inQuerying_;
  long lastQueryTime_;
  int backupRequestDelayMs;
  private TableInterceptorManager interceptorManager;

  public TableHandler(ClusterManager mgr, String name, InternalTableOptions internalTableOptions)
      throws ReplicationException {
    int i = 0;
    for (; i < name.length(); i++) {
      char c = name.charAt(i);
      if ((c >= 'a' && c <= 'z')
          || (c >= 'A' && c <= 'Z')
          || (c >= '0' && c <= '9')
          || c == '_'
          || c == '.'
          || c == ':') continue;
      else break;
    }
    if (name.length() > 0 && i == name.length()) {
      logger.info(
          "initialize table handler, table name is \"{}\"", StringEscapeUtils.escapeJava(name));
    } else {
      logger.warn(
          "initialize table handler, maybe invalid table name \"{}\"",
          StringEscapeUtils.escapeJava(name));
    }

    query_cfg_request req = new query_cfg_request(name, new ArrayList<Integer>());
    query_cfg_operator op = new query_cfg_operator(new gpid(-1, -1), req);
    mgr.getMetaSession().execute(op, 5);
    error_code.error_types err = MetaSession.getMetaServiceError(op);
    if (err != error_code.error_types.ERR_OK) {
      handleMetaException(err, mgr, name);
      return;
    }
    query_cfg_response resp = op.get_response();
    logger.info(
        "query meta configuration succeed, table_name({}), app_id({}), partition_count({})",
        name,
        resp.app_id,
        resp.partition_count);

    // superclass members
    tableName_ = name;
    appID_ = resp.app_id;
    hasher_ = internalTableOptions.keyHasher();

    // members of this
    manager_ = mgr;
    executor_ = manager_.getExecutor();
    this.backupRequestDelayMs = internalTableOptions.tableOptions().backupRequestDelayMs();
    if (backupRequestDelayMs > 0) {
      logger.info("the delay time of backup request is \"{}\"", backupRequestDelayMs);
    }

    tableConfig_ = new AtomicReference<TableConfiguration>(null);
    initTableConfiguration(resp);

    inQuerying_ = new AtomicBoolean(false);
    lastQueryTime_ = 0;

    this.interceptorManager = new TableInterceptorManager(internalTableOptions.tableOptions());
  }

  public ReplicaConfiguration getReplicaConfig(int index) {
    return tableConfig_.get().replicas.get(index);
  }

  public gpid getGpidByHash(long hashValue) {
    int index = (int) remainder_unsigned(hashValue, getPartitionCount());
    final ReplicaConfiguration replicaConfiguration = this.getReplicaConfig(index);
    // table is partition split, and child partition is not ready
    // child requests should be redirected to its parent partition
    if (replicaConfiguration.ballot < 0) {
      logger.info(
          "Table[{}] is executing partition split, partition[{}] is not ready, requests will send to parent partition[{}]",
          tableName_,
          index,
          index - getPartitionCount() / 2);
      index -= getPartitionCount() / 2;
    }
    return new gpid(appID_, index);
  }

  // update the table configuration & appID_ according to to queried response
  // there should only be one thread to do the table config update
  void initTableConfiguration(query_cfg_response resp) {
    int partitionCount = resp.getPartition_count();
    TableConfiguration oldConfig = tableConfig_.get();

    TableConfiguration newConfig = new TableConfiguration();
    newConfig.updateVersion = (oldConfig == null) ? 1 : (oldConfig.updateVersion + 1);
    newConfig.replicas = new ArrayList<>(partitionCount);
    for (int i = 0; i != partitionCount; ++i) {
      ReplicaConfiguration newReplicaConfig = new ReplicaConfiguration();
      newReplicaConfig.pid.set_app_id(resp.getApp_id());
      newReplicaConfig.pid.set_pidx(i);
      newConfig.replicas.add(newReplicaConfig);
    }

    // create sessions for primary and secondaries
    FutureGroup<Void> futureGroup = new FutureGroup<>(partitionCount);
    for (partition_configuration pc : resp.getPartitions()) {
      int index = pc.getPid().get_pidx();
      ReplicaConfiguration replicaConfig = newConfig.replicas.get(index);
      replicaConfig.ballot = pc.ballot;

      // table is partition split, and child partition is not ready
      // child requests should be redirected to its parent partition
      // this will be happened when query meta is called during partition split
      if (replicaConfig.ballot < 0) {
        continue;
      }

      replicaConfig.primaryAddress = pc.getPrimary();
      // If the primary address is invalid, we don't create secondary session either.
      // Because all of these sessions will be recreated later.
      if (replicaConfig.primaryAddress.isInvalid()) {
        continue;
      }
      replicaConfig.primarySession = tryConnect(replicaConfig.primaryAddress, futureGroup);

      replicaConfig.secondarySessions.clear();
      // backup request is enabled, get all secondary sessions
      if (isBackupRequestEnabled()) {
        // secondary sessions
        pc.secondaries.forEach(
            secondary -> {
              ReplicaSession session = tryConnect(secondary, futureGroup);
              if (session != null) {
                replicaConfig.secondarySessions.add(session);
              }
            });
      }
    }

    // Warm up the connections during client.openTable, so RPCs thereafter can
    // skip the connect process.
    try {
      futureGroup.waitAllCompleteOrOneFail(manager_.getTimeout());
    } catch (PException e) {
      logger.warn("failed to connect with some replica servers: ", e);
    }

    // there should only be one thread to do the table config update
    appID_ = resp.getApp_id();
    tableConfig_.set(newConfig);
  }

  public ReplicaSession tryConnect(final rpc_address addr, FutureGroup<Void> futureGroup) {
    if (addr.isInvalid()) {
      return null;
    }

    ReplicaSession session = manager_.getReplicaSession(addr);
    ChannelFuture fut = session.tryConnect();
    if (fut != null) {
      futureGroup.add(fut);
    }

    return session;
  }

  boolean isPartitionCountValid(int old_count, int resp_count) {
    return ((old_count == resp_count) // normal case
        || (old_count * 2 == resp_count) // table start partition split
        || (old_count == resp_count * 2) // table partition split cancel
    );
  }

  void onUpdateConfiguration(final query_cfg_operator op) {
    error_code.error_types err = MetaSession.getMetaServiceError(op);
    if (err != error_code.error_types.ERR_OK) {
      logger.warn("query meta for table({}) failed, error_code({})", tableName_, err.toString());
    } else {
      logger.info("query meta for table({}) received response", tableName_);
      query_cfg_response resp = op.get_response();
      if (resp.app_id != appID_
          || !isPartitionCountValid(tableConfig_.get().replicas.size(), resp.partition_count)) {
        logger.warn(
            "table({}) meta reset, app_id({}->{}), partition_count({}->{})",
            tableName_,
            appID_,
            resp.app_id,
            tableConfig_.get().replicas.size(),
            resp.partition_count);
      }
      initTableConfiguration(resp);
    }

    inQuerying_.set(false);
  }

  boolean tryQueryMeta(long cachedConfigVersion) {
    if (!inQuerying_.compareAndSet(false, true)) return false;

    long now = System.currentTimeMillis();
    if (now - lastQueryTime_ < Math.max(1, manager_.getTimeout() / 3)) {
      inQuerying_.set(false);
      return false;
    }
    if (tableConfig_.get().updateVersion > cachedConfigVersion) {
      inQuerying_.set(false);
      return false;
    }

    lastQueryTime_ = now;
    query_cfg_request req = new query_cfg_request(tableName_, new ArrayList<Integer>());
    final query_cfg_operator query_op = new query_cfg_operator(new gpid(-1, -1), req);

    logger.info("query meta for table({}) query request", tableName_);
    manager_
        .getMetaSession()
        .asyncExecute(
            query_op,
            new Runnable() {
              @Override
              public void run() {
                onUpdateConfiguration(query_op);
              }
            },
            5);

    return true;
  }

  public void onRpcReply(ClientRequestRound round, long cachedConfigVersion, String serverAddr) {
    // judge if it is the first response
    if (round.isCompleted) {
      return;
    } else {
      synchronized (TableHandler.class) {
        // the fastest response has been received
        if (round.isCompleted) {
          return;
        }
        round.isCompleted = true;
      }
    }

    client_operator operator = round.getOperator();
    interceptorManager.after(round, operator.rpc_error.errno, this);
    boolean needQueryMeta = false;
    switch (operator.rpc_error.errno) {
      case ERR_OK:
        round.thisRoundCompletion();
        return;

        // timeout
      case ERR_TIMEOUT: // <- operation timeout
        logger.warn(
            "{}: replica server({}) rpc timeout for gpid({}), operator({}), try({}), error_code({}), not retry",
            tableName_,
            serverAddr,
            operator.get_gpid().toString(),
            operator,
            round.tryId,
            operator.rpc_error.errno.toString());
        break;

        // under these cases we should query the new config from meta and retry later
      case ERR_SESSION_RESET: // <- connection with the server failed
      case ERR_OBJECT_NOT_FOUND: // <- replica server doesn't serve this gpid
      case ERR_INVALID_STATE: // <- replica server is not primary
      case ERR_PARENT_PARTITION_MISUSED: // <- send request to wrong partition because of partition
        // split
        logger.warn(
            "{}: replica server({}) doesn't serve gpid({}), operator({}), try({}), error_code({}), need query meta",
            tableName_,
            serverAddr,
            operator.get_gpid().toString(),
            operator,
            round.tryId,
            operator.rpc_error.errno.toString());
        needQueryMeta = true;
        break;

        // under these cases we should retry later without querying the new config from meta
      case ERR_NOT_ENOUGH_MEMBER:
      case ERR_CAPACITY_EXCEEDED:
        logger.warn(
            "{}: replica server({}) can't serve writing for gpid({}), operator({}), try({}), error_code({}), retry later",
            tableName_,
            serverAddr,
            operator.get_gpid().toString(),
            operator,
            round.tryId,
            operator.rpc_error.errno.toString());
        break;

        // under other cases we should not retry
      default:
        logger.error(
            "{}: replica server({}) fails for gpid({}), operator({}), try({}), error_code({}), not retry",
            tableName_,
            serverAddr,
            operator.get_gpid().toString(),
            operator,
            round.tryId,
            operator.rpc_error.errno.toString());
        round.thisRoundCompletion();
        return;
    }

    if (needQueryMeta) {
      tryQueryMeta(cachedConfigVersion);
    }

    // must use new round here, because round.isSuccess is true now
    // must use round.expireNanoTime to init, otherwise "round.expireNanoTime - System.nanoTime() >
    // nanoDelay" in "tryDelayCall()" will be always true
    ClientRequestRound delayRequestRound =
        new ClientRequestRound(
            round.operator,
            round.callback,
            round.enableCounter,
            round.expireNanoTime,
            round.timeoutMs,
            round.tryId);
    tryDelayCall(delayRequestRound);
  }

  private void tryDelayCall(final ClientRequestRound round) {
    // tryId starts from 1 so here we minus 1
    RetryPolicy.RetryAction action =
        manager_
            .getRetryPolicy()
            .shouldRetry(round.tryId - 1, round.expireNanoTime, Duration.ofMillis(round.timeoutMs));
    if (action.getDecision() == RetryPolicy.RetryDecision.RETRY) {
      logger.debug(
          "retry policy {} decided to retry after {} for operation with hashcode {},"
              + " retry = {}",
          manager_.getRetryPolicy().getClass().getSimpleName(),
          action.getDelay(),
          System.identityHashCode(round.getOperator()),
          round.tryId);
      round.tryId++;
      executor_.schedule(
          new Runnable() {
            @Override
            public void run() {
              call(round);
            }
          },
          action.getDelay().toNanos(),
          TimeUnit.NANOSECONDS);
    } else {
      logger.debug(
          "retry policy {} decided to fail for operation with hashcode {}," + " retry = {}",
          manager_.getRetryPolicy().getClass().getSimpleName(),
          System.identityHashCode(round.getOperator()),
          round.tryId);
      // errno == ERR_UNKNOWN means the request has never attemptted to contact any replica servers
      // this may happen when we can't initialize a null replica session for a long time
      if (round.getOperator().rpc_error.errno == error_code.error_types.ERR_UNKNOWN) {
        round.getOperator().rpc_error.errno = error_code.error_types.ERR_TIMEOUT;
      }
      round.thisRoundCompletion();
    }
  }

  void call(final ClientRequestRound round) {
    // tableConfig & handle is initialized in constructor, so both shouldn't be null
    final TableConfiguration tableConfig = tableConfig_.get();
    final ReplicaConfiguration handle =
        tableConfig.replicas.get(round.getOperator().get_gpid().get_pidx());

    if (handle.primarySession != null) {
      interceptorManager.before(round, this);
      // send request to primary
      handle.primarySession.asyncSend(
          round.getOperator(),
          new Runnable() {
            @Override
            public void run() {
              onRpcReply(round, tableConfig.updateVersion, handle.primarySession.name());
            }
          },
          round.timeoutMs,
          false);
    } else {
      logger.warn(
          "{}: no primary for gpid({}), operator({}), try({}), retry later",
          tableName_,
          round.getOperator().get_gpid().toString(),
          round.getOperator(),
          round.tryId);
      tryQueryMeta(tableConfig.updateVersion);
      tryDelayCall(round);
    }
  }

  @Override
  public int getPartitionCount() {
    return tableConfig_.get().replicas.size();
  }

  @Override
  public void operate(client_operator op, int timeoutMs) throws ReplicationException {
    final FutureTask<Void> syncer =
        new FutureTask<Void>(
            new Callable<Void>() {
              @Override
              public Void call() throws Exception {
                return null;
              }
            });
    ClientOPCallback cb =
        new ClientOPCallback() {
          @Override
          public void onCompletion(client_operator op) throws Throwable {
            syncer.run();
          }
        };

    asyncOperate(op, cb, timeoutMs);

    try {
      syncer.get(timeoutMs, TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException e) {
      logger.info("got exception: " + e);
      throw new ReplicationException(e);
    } catch (TimeoutException e) {
      op.rpc_error.errno = error_code.error_types.ERR_TIMEOUT;
    }

    if (op.rpc_error.errno != error_code.error_types.ERR_OK) {
      throw new ReplicationException(op.rpc_error.errno);
    }
  }

  public int backupRequestDelayMs() {
    return backupRequestDelayMs;
  }

  public long updateVersion() {
    return tableConfig_.get().updateVersion;
  }

  @Override
  public EventExecutor getExecutor() {
    return executor_;
  }

  @Override
  public int getDefaultTimeout() {
    return manager_.getTimeout();
  }

  @Override
  public void asyncOperate(client_operator op, ClientOPCallback callback, int timeoutMs) {
    if (timeoutMs <= 0) {
      timeoutMs = manager_.getTimeout();
    }

    ClientRequestRound round =
        new ClientRequestRound(op, callback, manager_.counterEnabled(), (long) timeoutMs);
    call(round);
  }

  private void handleMetaException(error_code.error_types err_type, ClusterManager mgr, String name)
      throws ReplicationException {
    String metaServer = Arrays.toString(mgr.getMetaList());
    String message = "";
    String header = "[metaServer=" + metaServer + ",tableName=" + name + "]";
    switch (err_type) {
      case ERR_OBJECT_NOT_FOUND:
        message =
            " No such table. Please make sure your meta addresses and table name are correct!";
        break;
      case ERR_BUSY_CREATING:
        message = " The table is creating, please wait a moment and retry it!";
        break;
      case ERR_BUSY_DROPPING:
        message = " The table is dropping, please confirm the table name!";
        break;
      case ERR_SESSION_RESET:
        message = " Unable to connect to the meta servers!";
        break;
      default:
        message = " Unknown error!";
        break;
    }
    throw new ReplicationException(err_type, header + message);
  }

  private boolean isBackupRequestEnabled() {
    return backupRequestDelayMs > 0;
  }
}
