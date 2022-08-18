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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.socket.SocketChannel;
import java.net.UnknownHostException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.rpc_address;
import org.apache.pegasus.operator.ClientOperator;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptorManager;
import org.apache.thrift.protocol.TMessage;
import org.slf4j.Logger;

public class ReplicaSession {
  public static class RequestEntry {
    int sequenceId;
    public ClientOperator op;
    public Runnable callback;
    public ScheduledFuture<?> timeoutTask;
    public long timeoutMs;
    public boolean isBackupRequest;
  }

  public enum ConnState {
    CONNECTED,
    CONNECTING,
    DISCONNECTED
  }

  public ReplicaSession(
      rpc_address address,
      EventLoopGroup rpcGroup,
      EventLoopGroup timeoutTaskGroup,
      int socketTimeout,
      long sessionResetTimeWindowSec,
      ReplicaSessionInterceptorManager interceptorManager) {
    this.address = address;
    this.timeoutTaskGroup = timeoutTaskGroup;
    this.interceptorManager = interceptorManager;
    this.sessionResetTimeWindowMs = sessionResetTimeWindowSec * 1000;

    final ReplicaSession this_ = this;
    boot = new Bootstrap();
    boot.group(rpcGroup)
        .channel(ClusterManager.getSocketChannelClass())
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, socketTimeout)
        .handler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast("ThriftEncoder", new ThriftFrameEncoder());
                pipeline.addLast("ThriftDecoder", new ThriftFrameDecoder(this_));
                pipeline.addLast("ClientHandler", new ReplicaSession.DefaultHandler());
              }
            });

    this.firstRecentTimedOutMs = new AtomicLong(0);
  }

  void setMessageResponseFilter(MessageResponseFilter filter) {
    this.filter = filter;
  }

  public void asyncSend(
      ClientOperator op,
      Runnable callbackFunc,
      long timeoutInMilliseconds,
      boolean isBackupRequest) {
    RequestEntry entry = new RequestEntry();
    entry.sequenceId = seqId.getAndIncrement();
    entry.op = op;
    entry.callback = callbackFunc;
    // NOTICE: must make sure the msg is put into the pendingResponse map BEFORE
    // the timer task is scheduled.
    pendingResponse.put(entry.sequenceId, entry);
    entry.timeoutTask = addTimer(entry.sequenceId, timeoutInMilliseconds);
    entry.timeoutMs = timeoutInMilliseconds;
    entry.isBackupRequest = isBackupRequest;

    // We store the connection_state & netty channel in a struct so that they can fetch and update
    // in atomic.
    // Moreover, we can avoid the lock protection when we want to get the netty channel for send
    // message
    VolatileFields cache = fields;
    if (cache.state == ConnState.CONNECTED) {
      write(entry, cache);
    } else {
      synchronized (pendingSend) {
        cache = fields;
        if (cache.state == ConnState.CONNECTED) {
          write(entry, cache);
        } else {
          if (!pendingSend.offer(entry)) {
            logger.warn("pendingSend queue is full, drop the request");
          }
        }
      }
      tryConnect();
    }
  }

  public void closeSession() {
    VolatileFields f = fields;
    if (f.state == ConnState.CONNECTED && f.nettyChannel != null) {
      try {
        // close().sync() means calling system API `close()` synchronously,
        // but the connection may not be completely closed then, that is,
        // the state may not be marked as DISCONNECTED immediately.
        f.nettyChannel.close().sync();
        logger.info("channel to {} closed", address.toString());
      } catch (Exception ex) {
        logger.warn("close channel {} failed: ", address.toString(), ex);
      }
    } else if (f.state == ConnState.CONNECTING) { // f.nettyChannel == null
      // If our actively-close strategy fails to reconnect the session due to
      // some sort of deadlock, close this session and retry.
      logger.info("{}: close a connecting session", name());
      markSessionDisconnect();
    } else {
      logger.info(
          "{}: session is not connected [state={}, nettyChannel{}=null], skip the close",
          name(),
          f.state,
          f.nettyChannel == null ? "=" : "!");
    }
  }

  public RequestEntry getAndRemoveEntry(int seqID) {
    return pendingResponse.remove(seqID);
  }

  public final String name() {
    return address.toString();
  }

  public final rpc_address getAddress() {
    return address;
  }

  @Override
  public String toString() {
    return address.toString();
  }

  /**
   * Connects to remote host if it is currently disconnected.
   *
   * @return a nullable ChannelFuture.
   */
  public ChannelFuture tryConnect() {
    boolean needConnect = false;
    synchronized (pendingSend) {
      if (fields.state == ConnState.DISCONNECTED) {
        VolatileFields cache = new VolatileFields();
        cache.state = ConnState.CONNECTING;
        fields = cache;
        needConnect = true;
      }
    }
    if (needConnect) {
      logger.info("{}: the session is disconnected, needs to reconnect", name());
      return doConnect();
    }
    return null;
  }

  private ChannelFuture doConnect() {
    try {
      // we will receive the channel connect event in DefaultHandler.ChannelActive
      return boot.connect(address.get_ip(), address.get_port())
          .addListener(
              new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture channelFuture) throws Exception {
                  if (channelFuture.isSuccess()) {
                    logger.info(
                        "{}: start to async connect to target, wait channel to active", name());
                  } else {
                    logger.warn(
                        "{}: try to connect to target failed: ", name(), channelFuture.cause());
                    markSessionDisconnect();
                  }
                }
              });
    } catch (UnknownHostException ex) {
      logger.error("invalid address: {}", address.toString());
      assert false;
      return null; // unreachable
    }
  }

  private void markSessionConnected(Channel activeChannel) {
    VolatileFields newCache = new VolatileFields();
    newCache.state = ConnState.CONNECTED;
    newCache.nettyChannel = activeChannel;

    interceptorManager.onConnected(this);

    synchronized (pendingSend) {
      if (fields.state != ConnState.CONNECTING) {
        // this session may have been closed or connected already
        logger.info("{}: session is {}, skip to mark it connected", name(), fields.state);
        return;
      }

      while (!pendingSend.isEmpty()) {
        RequestEntry e = pendingSend.poll();
        if (pendingResponse.get(e.sequenceId) != null) {
          write(e, newCache);
        } else {
          logger.info("{}: {} is removed from pending, perhaps timeout", name(), e.sequenceId);
        }
      }
      fields = newCache;
    }
  }

  void markSessionDisconnect() {
    VolatileFields cache = fields;
    synchronized (pendingSend) {
      if (cache.state != ConnState.DISCONNECTED) {
        // NOTICE:
        // 1. when a connection is reset, the timeout response
        // is not answered in the order they query
        // 2. It's likely that when the session is disconnecting
        // but the caller of the api query/asyncQuery didn't notice
        // this. In this case, we are relying on the timeout task.
        try {
          while (!pendingSend.isEmpty()) {
            RequestEntry e = pendingSend.poll();
            tryNotifyFailureWithSeqID(
                e.sequenceId, error_code.error_types.ERR_SESSION_RESET, false);
          }
          List<RequestEntry> l = new LinkedList<RequestEntry>();
          for (Map.Entry<Integer, RequestEntry> entry : pendingResponse.entrySet()) {
            l.add(entry.getValue());
          }
          for (RequestEntry e : l) {
            tryNotifyFailureWithSeqID(
                e.sequenceId, error_code.error_types.ERR_SESSION_RESET, false);
          }
        } catch (Exception e) {
          logger.error(
              "failed to notify callers due to unexpected exception [state={}]: ",
              cache.state.toString(),
              e);
        } finally {
          logger.info("{}: mark the session to be disconnected from state={}", name(), cache.state);
          // ensure the state must be set DISCONNECTED
          cache = new VolatileFields();
          cache.state = ConnState.DISCONNECTED;
          cache.nettyChannel = null;
          fields = cache;
        }
      } else {
        logger.warn("{}: session is closed already", name());
      }
    }
  }

  // Notify the RPC sender if failure occurred.
  void tryNotifyFailureWithSeqID(int seqID, error_code.error_types errno, boolean isTimeoutTask)
      throws Exception {
    logger.debug(
        "{}: {} is notified with error {}, isTimeoutTask {}",
        name(),
        seqID,
        errno.toString(),
        isTimeoutTask);
    RequestEntry entry = pendingResponse.remove(seqID);
    if (entry != null) {
      if (!isTimeoutTask && entry.timeoutTask != null) {
        entry.timeoutTask.cancel(true);
      }
      if (errno == error_code.error_types.ERR_TIMEOUT) {
        long firstTs = firstRecentTimedOutMs.get();
        if (firstTs == 0) {
          // it is the first timeout in the window.
          firstRecentTimedOutMs.set(System.currentTimeMillis());
        } else if (System.currentTimeMillis() - firstTs >= sessionResetTimeWindowMs) {
          // ensure that closeSession() will be invoked only once.
          if (firstRecentTimedOutMs.compareAndSet(firstTs, 0)) {
            logger.warn(
                "{}: actively close the session because it's not responding for {} seconds",
                name(),
                sessionResetTimeWindowMs / 1000);
            closeSession(); // maybe fail when the session is already disconnected.
            errno = error_code.error_types.ERR_SESSION_RESET;
          }
        }
      } else {
        firstRecentTimedOutMs.set(0);
      }
      entry.op.rpc_error.errno = errno;
      entry.callback.run();
    } else {
      logger.warn(
          "{}: {} is removed by others, current error {}, isTimeoutTask {}",
          name(),
          seqID,
          errno.toString(),
          isTimeoutTask);
    }
  }

  private void write(final RequestEntry entry, VolatileFields cache) {
    if (!interceptorManager.onSendMessage(this, entry)) {
      return;
    }

    cache
        .nettyChannel
        .writeAndFlush(entry)
        .addListener(
            new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                // NOTICE: we never do the connection things, this should be the duty of
                // ChannelHandler, we only notify the request
                if (!channelFuture.isSuccess()) {
                  logger.info(
                      "{} write seqid {} failed: ",
                      name(),
                      entry.sequenceId,
                      channelFuture.cause());
                  tryNotifyFailureWithSeqID(
                      entry.sequenceId, error_code.error_types.ERR_TIMEOUT, false);
                }
              }
            });
  }

  // Notify the RPC caller when times out. If the RPC finishes in time,
  // this task will be cancelled.
  // TODO(wutao1): call it addTimeoutTicker
  private ScheduledFuture<?> addTimer(final int seqID, long timeoutInMillseconds) {
    return this.timeoutTaskGroup.schedule(
        new Runnable() {
          @Override
          public void run() {
            try {
              tryNotifyFailureWithSeqID(seqID, error_code.error_types.ERR_TIMEOUT, true);
            } catch (Exception e) {
              logger.warn("try notify with sequenceID {} exception!", seqID, e);
            }
          }
        },
        timeoutInMillseconds,
        TimeUnit.MILLISECONDS);
  }

  public void onAuthSucceed() {
    Queue<RequestEntry> swappedPendingSend = new LinkedList<>();
    synchronized (authPendingSend) {
      authSucceed = true;
      swappedPendingSend.addAll(authPendingSend);
      authPendingSend.clear();
    }

    while (!swappedPendingSend.isEmpty()) {
      RequestEntry e = swappedPendingSend.poll();
      if (pendingResponse.get(e.sequenceId) != null) {
        write(e, fields);
      } else {
        logger.info("{}: {} is removed from pending, perhaps timeout", name(), e.sequenceId);
      }
    }
  }

  // return value:
  //   true  - pend succeed
  //   false - pend failed
  public boolean tryPendRequest(RequestEntry entry) {
    // double check. the first one doesn't lock the lock.
    // Because authSucceed only transfered from false to true.
    // So if it is true now, it will not change in the later.
    // But if it is false now, maybe it will change soon. So we should use lock to protect it.
    if (!this.authSucceed) {
      synchronized (authPendingSend) {
        if (!this.authSucceed) {
          if (!authPendingSend.offer(entry)) {
            logger.warn("{}: pend request {} failed", name(), entry.sequenceId);
          }
          return true;
        }
      }
    }

    return false;
  }

  final class DefaultHandler extends SimpleChannelInboundHandler<RequestEntry> {
    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      logger.warn("Channel {} for session {} is inactive", ctx.channel().toString(), name());
      markSessionDisconnect();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      logger.info("Channel {} for session {} is active", ctx.channel().toString(), name());
      markSessionConnected(ctx.channel());
    }

    @Override
    public void channelRead0(ChannelHandlerContext ctx, final RequestEntry msg) {
      logger.debug("{}: handle response with seqid({})", name(), msg.sequenceId);
      firstRecentTimedOutMs.set(0); // This session is currently healthy.
      if (msg.callback != null) {
        msg.callback.run();
      } else {
        logger.warn(
            "{}: seqid({}) has no callback, just ignore the response", name(), msg.sequenceId);
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
      logger.warn(
          "got exception in inbound handler {} for session {}: ",
          ctx.channel().toString(),
          name(),
          cause);
      ctx.close();
    }
  }

  // for test
  ConnState getState() {
    return fields.state;
  }

  interface MessageResponseFilter {
    boolean abandonIt(error_code.error_types err, TMessage header);
  }

  MessageResponseFilter filter = null;

  final ConcurrentHashMap<Integer, RequestEntry> pendingResponse =
      new ConcurrentHashMap<Integer, RequestEntry>();
  private final AtomicInteger seqId = new AtomicInteger(0);

  final Queue<RequestEntry> pendingSend = new LinkedList<RequestEntry>();

  static final class VolatileFields {
    public ConnState state = ConnState.DISCONNECTED;
    public Channel nettyChannel = null;
  }

  volatile VolatileFields fields = new VolatileFields();

  private final rpc_address address;
  private Bootstrap boot;
  private EventLoopGroup timeoutTaskGroup;
  private ReplicaSessionInterceptorManager interceptorManager;
  private volatile boolean authSucceed;
  final Queue<RequestEntry> authPendingSend = new LinkedList<>();

  // Session will be actively closed if all the rpcs across `sessionResetTimeWindowMs`
  // are timed out, in that case we suspect that the server is unavailable.

  // Timestamp of the first timed out rpc.
  private final AtomicLong firstRecentTimedOutMs;
  private final long sessionResetTimeWindowMs;

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSession.class);
}
