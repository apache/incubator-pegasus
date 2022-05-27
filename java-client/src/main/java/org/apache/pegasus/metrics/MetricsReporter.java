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
package org.apache.pegasus.metrics;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.json.JSONException;
import org.slf4j.Logger;

/** Created by weijiesun on 18-3-9. */
public class MetricsReporter {
  public MetricsReporter(int reportSecs, MetricsPool pool) {
    falconAgentIP = "127.0.0.1";
    falconAgentPort = 1988;
    falconAgentSocket = falconAgentIP + ":" + String.valueOf(falconAgentPort);

    reportIntervalSecs = reportSecs;
    falconRequestPath = "/v1/push";
    metrics = pool;

    boot = new Bootstrap();
    httpClientGroup = new NioEventLoopGroup(1);

    boot.group(httpClientGroup)
        .channel(NioSocketChannel.class)
        .option(ChannelOption.TCP_NODELAY, true)
        .option(ChannelOption.SO_KEEPALIVE, true)
        .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 500)
        .handler(
            new ChannelInitializer<SocketChannel>() {
              @Override
              public void initChannel(SocketChannel ch) {
                ChannelPipeline p = ch.pipeline();
                p.addLast(new HttpClientCodec());
                p.addLast(new HttpContentDecompressor());
                p.addLast(new HttpClientHandler());
              }
            });

    actionLater = null;
    reportStopped = true;
    reportTarget = null;
  }

  public void start() {
    reportStopped = false;
    tryConnect();
  }

  public void stop() {
    httpClientGroup.execute(
        new Runnable() {
          @Override
          public void run() {
            reportStopped = true;
            if (actionLater != null) {
              actionLater.cancel(false);
            }
            if (reportTarget != null) {
              reportTarget
                  .close()
                  .addListener(
                      new ChannelFutureListener() {
                        @Override
                        public void operationComplete(ChannelFuture channelFuture)
                            throws Exception {
                          if (channelFuture.isSuccess()) {
                            logger.info("close channel to {} succeed", falconAgentSocket);
                          } else {
                            logger.warn("close channel to {} failed: ", channelFuture.cause());
                          }
                        }
                      });
            }
          }
        });

    try {
      httpClientGroup.shutdownGracefully().sync();
      logger.info("close metrics reporter");
    } catch (Exception ex) {
      logger.warn("close metrics report failed: ", ex);
    }
  }

  public void tryConnect() {
    boot.connect(falconAgentIP, falconAgentPort)
        .addListener(
            new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (channelFuture.isSuccess()) {
                  reportTarget = channelFuture.channel();
                  logger.info("create channel with {} succeed, wait it active", falconAgentSocket);
                } else {
                  logger.error(
                      "create channel with {} failed, connect later: ",
                      falconAgentSocket,
                      channelFuture.cause());
                  scheduleNextConnect();
                }
              }
            });
  }

  public void scheduleNextConnect() {
    if (reportStopped) return;
    actionLater =
        httpClientGroup.schedule(
            new Runnable() {
              @Override
              public void run() {
                tryConnect();
              }
            },
            (long) reportIntervalSecs,
            TimeUnit.SECONDS);
  }

  public void scheduleNextReport(final Channel channel) {
    if (reportStopped) return;
    actionLater =
        httpClientGroup.schedule(
            new Runnable() {
              @Override
              public void run() {
                reportMetrics(channel);
              }
            },
            reportIntervalSecs,
            TimeUnit.SECONDS);
  }

  public void reportMetrics(final Channel channel) {
    String json_metrics;
    try {
      json_metrics = metrics.metricsToJson();
    } catch (JSONException ex) {
      logger.warn("encode metrics to json failed, skip current report, retry later: ", ex);
      scheduleNextReport(channel);
      return;
    }

    logger.debug("generate metrics {} and try to report", json_metrics);
    FullHttpRequest request =
        new DefaultFullHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.POST,
            falconRequestPath,
            Unpooled.copiedBuffer(json_metrics.getBytes()));
    request.headers().add(HttpHeaders.Names.HOST, falconAgentSocket);
    request.headers().add(HttpHeaders.Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
    request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, request.content().readableBytes());
    request.headers().add(HttpHeaders.Names.CONTENT_TYPE, "application/x-www-form-urlencoded");

    channel
        .writeAndFlush(request)
        .addListener(
            new ChannelFutureListener() {
              @Override
              public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (!channelFuture.isSuccess()) {
                  logger.warn(
                      "report to {} failed, skip current report, retry later: ",
                      channel.toString(),
                      channelFuture.cause());
                  channel.close();
                }
              }
            });
  }

  class HttpClientHandler extends SimpleChannelInboundHandler<HttpObject> {
    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject httpObject) throws Exception {
      if (httpObject instanceof HttpResponse) {
        HttpResponse resp = (HttpResponse) httpObject;
        if (!resp.getStatus().equals(HttpResponseStatus.OK)) {
          logger.error("http response status: {}", resp.getStatus().toString());
        }
        logger.debug("http response status {}: CONTENT:[", resp.getStatus().toString());
      }
      if (httpObject instanceof HttpContent) {
        HttpContent content = (HttpContent) httpObject;
        logger.debug(content.content().toString(CharsetUtil.UTF_8));

        if (content instanceof LastHttpContent) {
          logger.debug("], response finished, schedule next report");
          scheduleNextReport(ctx.channel());
        }
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      logger.error("{} exception got: ", ctx.channel().toString(), cause);
      ctx.close();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
      logger.info("channel {} is active", ctx.channel().toString());
      reportMetrics(ctx.channel());
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      logger.info("channel {} is inactive", ctx.channel().toString());
      reportTarget = null;
      scheduleNextConnect();
    }
  }

  private String falconAgentIP;
  private int falconAgentPort;
  private String falconAgentSocket; // IP:port;
  private int reportIntervalSecs;
  private String falconRequestPath;

  private MetricsPool metrics;

  private Bootstrap boot;
  private EventLoopGroup httpClientGroup;

  private ScheduledFuture actionLater;
  private boolean reportStopped;
  private Channel reportTarget;

  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(MetricsReporter.class);
}
