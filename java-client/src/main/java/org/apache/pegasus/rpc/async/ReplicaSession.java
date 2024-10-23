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
import org.apache.pegasus.operator.client_operator;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptorManager;
import org.apache.thrift.protocol.TMessage;
import org.slf4j.Logger;

public class ReplicaSession {
  public static class RequestEntry {
    int sequenceId;
    public client_operator op;
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
      client_operator op,
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
      return;
    }

    synchronized (pendingSend) {
      cache = fields;
      if (cache.state == ConnState.CONNECTED) {
        write(entry, cache);
      } else {
        if (!pendingSend.offer(entry)) {
          logger.warn("pendingSend queue is full for session {}, drop the request", name());
        }
      }
    }
    tryConnect();
  }

  public void closeSession() {
    VolatileFields f = fields;
    if (f.state == ConnState.CONNECTED && f.nettyChannel != null) {
      try {
        // close().sync() means calling system API `close()` synchronously,
        // but the connection may not be completely closed then, that is,
        // the state may not be marked as DISCONNECTED immediately.
        f.nettyChannel.close().sync();
        logger.info("channel to {} closed", name());
      } catch (Exception ex) {
        logger.warn("close channel {} failed: ", name(), ex);
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
        fields = new VolatileFields(ConnState.CONNECTING);
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
      logger.error("invalid address: {}", name());
      assert false;
      return null; // unreachable
    }
  }

  private void markSessionConnected(Channel activeChannel) {
    VolatileFields newCache = new VolatileFields(ConnState.CONNECTED, activeChannel);

    // Note that actions in interceptor such as Negotiation might send request by
    // ReplicaSession#asyncSend(), inside which ReplicaSession#tryConnect() would
    // also be called since current state of this session is still CONNECTING.
    // However, it would never create another connection, thus it is safe to do
    // `fields = newCache` at the end.
    interceptorManager.onConnected(this);

    synchronized (pendingSend) {
      if (fields.state != ConnState.CONNECTING) {
        // This session may have been closed or connected already.
        logger.info("{}: session is {}, skip to mark it connected", name(), fields.state);
        return;
      }

      // Once the authentication is enabled, any request except Negotiation such as
      // query_cfg_operator for meta would be cached in authPendingSend and sent after
      // Negotiation is successful. Negotiation would be performed first before any other
      // request for the reason that AuthProtocol#isAuthRequest() would return true for
      // negotiation_operator.
      sendPendingRequests(pendingSend, newCache);
      fields = newCache;
    }
  }

  void markSessionDisconnect() {
    VolatileFields cache = fields;
    if (cache.state == ConnState.DISCONNECTED) {
      logger.warn("{}: session is closed already", name());
      resetAuth();
      return;
    }

    synchronized (pendingSend) {
      // NOTICE:
      // 1. when a connection is reset, the timeout response
      // is not answered in the order they query
      // 2. It's likely that when the session is disconnecting
      // but the caller of the api query/asyncQuery didn't notice
      // this. In this case, we are relying on the timeout task.
      try {
        while (!pendingSend.isEmpty()) {
          RequestEntry entry = pendingSend.poll();
          tryNotifyFailureWithSeqID(
              entry.sequenceId, error_code.error_types.ERR_SESSION_RESET, false);
        }
        List<RequestEntry> pendingEntries = new LinkedList<>();
        for (Map.Entry<Integer, RequestEntry> entry : pendingResponse.entrySet()) {
          pendingEntries.add(entry.getValue());
        }
        for (RequestEntry entry : pendingEntries) {
          tryNotifyFailureWithSeqID(
              entry.sequenceId, error_code.error_types.ERR_SESSION_RESET, false);
        }
      } catch (Exception ex) {
        logger.error(
            "{}: failed to notify callers due to unexpected exception [state={}]: ",
            name(),
            cache.state.toString(),
            ex);
      } finally {
        logger.info("{}: mark the session to be disconnected from state={}", name(), cache.state);
        fields = new VolatileFields(ConnState.DISCONNECTED);
      }
    }

    // Reset the authentication once the connection is closed.
    resetAuth();
  }

  // After the authentication is reset, a new Negotiation would be launched.
  private void resetAuth() {
    int pendingSize;
    synchronized (authPendingSend) {
      authSucceed = false;
      pendingSize = authPendingSend.size();
    }

    logger.info(
        "authentication is reset for session {}, with still {} request entries pending",
        name(),
        pendingSize);
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
    // Under some circumstances requests are not allowed to be sent or delayed.
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
            } catch (Exception ex) {
              logger.warn("{}: try notify with sequenceID {} exception!", name(), seqID, ex);
            }
          }
        },
        timeoutInMillseconds,
        TimeUnit.MILLISECONDS);
  }

  public void onAuthSucceed() {
    Queue<RequestEntry> swappedPendingSend;
    synchronized (authPendingSend) {
      authSucceed = true;
      swappedPendingSend = new LinkedList<>(authPendingSend);
      authPendingSend.clear();
    }

    logger.info(
        "authentication is successful for session {}, then {} pending request entries would be sent",
        name(),
        swappedPendingSend.size());

    sendPendingRequests(swappedPendingSend, fields);
  }

  private void sendPendingRequests(Queue<RequestEntry> pendingEntries, VolatileFields cache) {
    while (!pendingEntries.isEmpty()) {
      RequestEntry entry = pendingEntries.poll();
      if (pendingResponse.get(entry.sequenceId) != null) {
        write(entry, cache);
      } else {
        logger.info("{}: {} is removed from pending, perhaps timeout", name(), entry.sequenceId);
      }
    }
  }

  // Return value:
  //   true  - pend succeed
  //   false - pend failed
  public boolean tryPendRequest(RequestEntry entry) {
    // Double check.
    if (this.authSucceed) {
      return false;
    }

    synchronized (authPendingSend) {
      if (this.authSucceed) {
        return false;
      }

      if (!authPendingSend.offer(entry)) {
        logger.warn("{}: pend request {} failed", name(), entry.sequenceId);
      }
    }

    return true;
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
      logger.trace("{}: handle response with seqid({})", name(), msg.sequenceId);
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

  // Only for test.
  ConnState getState() {
    return fields.state;
  }

  interface AuthPendingChecker {
    void onCheck(Queue<RequestEntry> realAuthPendingSend);
  }

  void checkAuthPending(AuthPendingChecker checker) {
    synchronized (authPendingSend) {
      checker.onCheck(authPendingSend);
    }
  }

  interface MessageResponseFilter {
    boolean abandonIt(error_code.error_types err, TMessage header);
  }

  MessageResponseFilter filter = null;

  final ConcurrentHashMap<Integer, RequestEntry> pendingResponse = new ConcurrentHashMap<>();
  private final AtomicInteger seqId = new AtomicInteger(0);

  final Queue<RequestEntry> pendingSend = new LinkedList<>();

  static final class VolatileFields {
    public VolatileFields(ConnState state, Channel nettyChannel) {
      this.state = state;
      this.nettyChannel = nettyChannel;
    }

    public VolatileFields(ConnState state) {
      this(state, null);
    }

    public ConnState state;
    public Channel nettyChannel;
  }

  volatile VolatileFields fields = new VolatileFields(ConnState.DISCONNECTED);

  private final rpc_address address;
  private final Bootstrap boot;
  private final EventLoopGroup timeoutTaskGroup;
  private final ReplicaSessionInterceptorManager interceptorManager;
  private volatile boolean authSucceed;
  private final Queue<RequestEntry> authPendingSend = new LinkedList<>();

  // Session will be actively closed if all the RPCs across `sessionResetTimeWindowMs`
  // are timed out, in that case we suspect that the server is unavailable.

  // Timestamp of the first timed out rpc.
  private final AtomicLong firstRecentTimedOutMs;
  private final long sessionResetTimeWindowMs;

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(ReplicaSession.class);
}
