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

import static java.lang.Integer.max;

import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.client.retry.DefaultRetryPolicy;
import org.apache.pegasus.client.retry.RetryPolicy;
import org.apache.pegasus.metrics.MetricsManager;
import org.apache.pegasus.rpc.Cluster;
import org.apache.pegasus.rpc.InternalTableOptions;
import org.apache.pegasus.rpc.ReplicationException;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptorManager;
import org.slf4j.Logger;

public class ClusterManager extends Cluster {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ClusterManager.class);

  private int operationTimeout;
  private long sessionResetTimeWindowSecs;
  private RetryPolicy retryPolicy;
  private boolean enableCounter;

  private ConcurrentHashMap<rpc_address, ReplicaSession> replicaSessions;
  private EventLoopGroup metaGroup; // group used for handle meta logic
  private EventLoopGroup replicaGroup; // group used for handle io with replica servers
  private EventLoopGroup timeoutTaskGroup; // group used for handle timeout task in replica servers
  private EventLoopGroup tableGroup; // group used for handle table logic
  private final String[] metaList;
  private MetaSession metaSession;
  private ReplicaSessionInterceptorManager sessionInterceptorManager;

  private volatile MetaHandler metaHandler = null;

  private static final String osName;

  static {
    Properties p = System.getProperties();
    osName = p.getProperty("os.name");
    logger.info("operating system name: {}", osName);
  }

  public ClusterManager(ClientOptions opts) throws IllegalArgumentException {
    setTimeout((int) opts.getOperationTimeout().toMillis());
    this.enableCounter = opts.isEnablePerfCounter();
    this.sessionResetTimeWindowSecs = opts.getSessionResetTimeWindowSecs();
    try {
      this.retryPolicy =
          opts.getRetryPolicy()
              .getImplementationClass()
              .getConstructor(ClientOptions.class)
              .newInstance(opts);
    } catch (Exception e) {
      logger.warn(
          "failed to create retry policy {}, use default retry policy instead",
          opts.getRetryPolicy(),
          e);
      this.retryPolicy = new DefaultRetryPolicy(opts);
    }
    if (enableCounter) {
      MetricsManager.detectHostAndInit(
          opts.getFalconPerfCounterTags(), (int) opts.getFalconPushInterval().getSeconds());
    }

    replicaSessions = new ConcurrentHashMap<rpc_address, ReplicaSession>();
    replicaGroup = getEventLoopGroupInstance(opts.getAsyncWorkers());
    timeoutTaskGroup = getEventLoopGroupInstance(opts.getAsyncWorkers());
    metaGroup = getEventLoopGroupInstance(1);
    tableGroup = getEventLoopGroupInstance(1);
    sessionInterceptorManager = new ReplicaSessionInterceptorManager(opts);

    metaList = opts.getMetaServers().split(",");
    // the constructor of meta session is depended on the replicaSessions,
    // so the replicaSessions should be initialized earlier
    metaSession =
        new MetaSession(this, metaList, (int) opts.getMetaQueryTimeout().toMillis(), 10, metaGroup);
  }

  public EventExecutor getExecutor() {
    return tableGroup.next();
  }

  public MetaSession getMetaSession() {
    return metaSession;
  }

  public ReplicaSession getReplicaSession(rpc_address address) {
    if (address.isInvalid()) {
      return null;
    }
    ReplicaSession ss = replicaSessions.get(address);
    if (ss != null) return ss;
    synchronized (this) {
      ss = replicaSessions.get(address);
      if (ss != null) return ss;
      ss =
          new ReplicaSession(
              address,
              replicaGroup,
              timeoutTaskGroup,
              max(operationTimeout, ClientOptions.MIN_SOCK_CONNECT_TIMEOUT),
              sessionResetTimeWindowSecs,
              sessionInterceptorManager);
      replicaSessions.put(address, ss);
      return ss;
    }
  }

  public int getTimeout() {
    return operationTimeout;
  }

  public long getRetryDelay(long timeoutMs) {
    return (timeoutMs < 3 ? 1 : timeoutMs / 3);
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }

  public boolean counterEnabled() {
    return enableCounter;
  }

  public void setTimeout(int t) {
    operationTimeout = t;
  }

  public static EventLoopGroup getEventLoopGroupInstance(int threadsCount) {
    logger.debug("create nio eventloop group");
    return new NioEventLoopGroup(threadsCount);
  }

  public static Class getSocketChannelClass() {
    logger.debug("create nio eventloop group");
    return NioSocketChannel.class;
  }

  @Override
  public String[] getMetaList() {
    return metaList;
  }

  @Override
  public TableHandler openTable(String name, InternalTableOptions internalTableOptions)
      throws ReplicationException {
    return new TableHandler(this, name, internalTableOptions);
  }

  @Override
  public MetaHandler getMeta() {
    if (null == metaHandler) {
      synchronized (this) {
        if (null == metaHandler) {
          metaHandler = new MetaHandler(this.metaSession);
        }
      }
    }

    return metaHandler;
  }

  @Override
  public void close() {
    sessionInterceptorManager.close();

    if (enableCounter) {
      MetricsManager.finish();
    }

    metaSession.closeSession();
    for (Map.Entry<rpc_address, ReplicaSession> entry : replicaSessions.entrySet()) {
      entry.getValue().closeSession();
    }

    Future metaGroupFuture = metaGroup.shutdownGracefully();
    Future replicaGroupFuture = replicaGroup.shutdownGracefully();
    Future tableGroupFuture = tableGroup.shutdownGracefully();
    Future timeoutTaskGroupFuture = timeoutTaskGroup.shutdownGracefully();

    try {
      metaGroupFuture.sync();
      logger.info("meta group has closed");
    } catch (Exception ex) {
      logger.warn("close meta group failed: ", ex);
    }

    try {
      replicaGroupFuture.sync();
      logger.info("replica group has closed");
    } catch (Exception ex) {
      logger.warn("close replica group failed: ", ex);
    }

    try {
      tableGroupFuture.sync();
      logger.info("table group has closed");
    } catch (Exception ex) {
      logger.warn("close table group failed: ", ex);
    }

    try {
      timeoutTaskGroupFuture.sync();
      logger.info("timeout task group has closed");
    } catch (Exception ex) {
      logger.warn("close timeout task group failed: ", ex);
    }

    logger.info("cluster manager has closed");
  }
}
