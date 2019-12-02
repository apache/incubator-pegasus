// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import java.time.Duration;

/**
 * Client Options to control the behavior of {@link PegasusClientInterface}.
 *
 * <p>To create a new instance with default settings:
 *
 * <pre>{@code
 * ClientOptions.create();
 * }</pre>
 *
 * To customize the settings:
 *
 * <pre>{@code
 * ClientOptions opts =
 *      ClientOptions.builder()
 *          .metaServers("127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603")
 *          .operationTimeout(Duration.ofMillis(1000))
 *          .asyncWorkers(4)
 *          .enablePerfCounter(false)
 *          .falconPerfCounterTags("")
 *          .falconPushInterval(Duration.ofSeconds(10))
 *          .build();
 * }</pre>
 */
public class ClientOptions {

  public static final String DEFAULT_META_SERVERS =
      "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
  public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofMillis(1000);
  public static final int DEFAULT_ASYNC_WORKERS = Runtime.getRuntime().availableProcessors();
  public static final boolean DEFAULT_ENABLE_PERF_COUNTER = false;
  public static final String DEFAULT_FALCON_PERF_COUNTER_TAGS = "";
  public static final Duration DEFAULT_FALCON_PUSH_INTERVAL = Duration.ofSeconds(10);

  private final String metaServers;
  private final Duration operationTimeout;
  private final int asyncWorkers;
  private final boolean enablePerfCounter;
  private final String falconPerfCounterTags;
  private final Duration falconPushInterval;

  protected ClientOptions(Builder builder) {
    this.metaServers = builder.metaServers;
    this.operationTimeout = builder.operationTimeout;
    this.asyncWorkers = builder.asyncWorkers;
    this.enablePerfCounter = builder.enablePerfCounter;
    this.falconPerfCounterTags = builder.falconPerfCounterTags;
    this.falconPushInterval = builder.falconPushInterval;
  }

  protected ClientOptions(ClientOptions original) {
    this.metaServers = original.getMetaServers();
    this.operationTimeout = original.getOperationTimeout();
    this.asyncWorkers = original.getAsyncWorkers();
    this.enablePerfCounter = original.isEnablePerfCounter();
    this.falconPerfCounterTags = original.getFalconPerfCounterTags();
    this.falconPushInterval = original.getFalconPushInterval();
  }

  /**
   * Create a copy of {@literal options}
   *
   * @param options the original
   * @return A new instance of {@link ClientOptions} containing the values of {@literal options}
   */
  public static ClientOptions copyOf(ClientOptions options) {
    return new ClientOptions(options);
  }

  /**
   * Returns a new {@link ClientOptions.Builder} to construct {@link ClientOptions}.
   *
   * @return a new {@link ClientOptions.Builder} to construct {@link ClientOptions}.
   */
  public static ClientOptions.Builder builder() {
    return new ClientOptions.Builder();
  }

  /**
   * Create a new instance of {@link ClientOptions} with default settings.
   *
   * @return a new instance of {@link ClientOptions} with default settings
   */
  public static ClientOptions create() {
    return builder().build();
  }

  @Override
  public boolean equals(Object options) {
    if (this == options) {
      return true;
    }
    if (options instanceof ClientOptions) {
      ClientOptions clientOptions = (ClientOptions) options;
      return this.metaServers.equals(clientOptions.metaServers)
          && this.operationTimeout.toMillis() == clientOptions.operationTimeout.toMillis()
          && this.asyncWorkers == clientOptions.asyncWorkers
          && this.enablePerfCounter == clientOptions.enablePerfCounter
          && this.falconPerfCounterTags.equals(clientOptions.falconPerfCounterTags)
          && this.falconPushInterval.toMillis() == clientOptions.falconPushInterval.toMillis();
    }
    return false;
  }

  @Override
  public String toString() {
    return "ClientOptions{"
        + "metaServers='"
        + metaServers
        + '\''
        + ", operationTimeout(ms)="
        + operationTimeout.toMillis()
        + ", asyncWorkers="
        + asyncWorkers
        + ", enablePerfCounter="
        + enablePerfCounter
        + ", falconPerfCounterTags='"
        + falconPerfCounterTags
        + '\''
        + ", falconPushInterval(s)="
        + falconPushInterval.getSeconds()
        + '}';
  }

  /** Builder for {@link ClientOptions}. */
  public static class Builder {
    private String metaServers = DEFAULT_META_SERVERS;
    private Duration operationTimeout = DEFAULT_OPERATION_TIMEOUT;
    private int asyncWorkers = DEFAULT_ASYNC_WORKERS;
    private boolean enablePerfCounter = DEFAULT_ENABLE_PERF_COUNTER;
    private String falconPerfCounterTags = DEFAULT_FALCON_PERF_COUNTER_TAGS;
    private Duration falconPushInterval = DEFAULT_FALCON_PUSH_INTERVAL;

    protected Builder() {}

    /**
     * The list of meta server addresses, separated by commas, See {@link #DEFAULT_META_SERVERS}.
     *
     * @param metaServers must not be {@literal null} or empty.
     * @return {@code this}
     */
    public Builder metaServers(String metaServers) {
      this.metaServers = metaServers;
      return this;
    }

    /**
     * The timeout for failing to finish an operation. Defaults to {@literal 1000ms}, see {@link
     * #DEFAULT_OPERATION_TIMEOUT}.
     *
     * @param operationTimeout operationTimeout
     * @return {@code this}
     */
    public Builder operationTimeout(Duration operationTimeout) {
      this.operationTimeout = operationTimeout;
      return this;
    }

    /**
     * The number of background worker threads. Internally it is the number of Netty NIO threads for
     * handling RPC events between client and Replica Servers. Defaults to {@literal 4}, see {@link
     * #DEFAULT_ASYNC_WORKERS}.
     *
     * @param asyncWorkers asyncWorkers thread number
     * @return {@code this}
     */
    public Builder asyncWorkers(int asyncWorkers) {
      this.asyncWorkers = asyncWorkers;
      return this;
    }

    /**
     * Whether to enable performance statistics. If true, the client will periodically report
     * metrics to local falcon agent (currently we only support falcon as monitoring system).
     * Defaults to {@literal false}, see {@link #DEFAULT_ENABLE_PERF_COUNTER}.
     *
     * @param enablePerfCounter enablePerfCounter
     * @return {@code this}
     */
    public Builder enablePerfCounter(boolean enablePerfCounter) {
      this.enablePerfCounter = enablePerfCounter;
      return this;
    }

    /**
     * Additional tags for falcon metrics. For example:
     * "cluster=c3srv-ad,job=recommend-service-history". Defaults to empty string, see {@link
     * #DEFAULT_FALCON_PERF_COUNTER_TAGS}.
     *
     * @param falconPerfCounterTags falconPerfCounterTags
     * @return {@code this}
     */
    public Builder falconPerfCounterTags(String falconPerfCounterTags) {
      this.falconPerfCounterTags = falconPerfCounterTags;
      return this;
    }

    /**
     * The interval to report metrics to local falcon agent. Defaults to {@literal 10s}, see {@link
     * #DEFAULT_FALCON_PUSH_INTERVAL}.
     *
     * @param falconPushInterval falconPushInterval
     * @return {@code this}
     */
    public Builder falconPushInterval(Duration falconPushInterval) {
      this.falconPushInterval = falconPushInterval;
      return this;
    }

    /**
     * Create a new instance of {@link ClientOptions}.
     *
     * @return new instance of {@link ClientOptions}.
     */
    public ClientOptions build() {
      return new ClientOptions(this);
    }
  }

  /**
   * Returns a builder to create new {@link ClientOptions} whose settings are replicated from the
   * current {@link ClientOptions}.
   *
   * @return a {@link ClientOptions.Builder} to create new {@link ClientOptions} whose settings are
   *     replicated from the current {@link ClientOptions}.
   */
  public ClientOptions.Builder mutate() {
    Builder builder = new Builder();
    builder
        .metaServers(getMetaServers())
        .operationTimeout(getOperationTimeout())
        .asyncWorkers(getAsyncWorkers())
        .enablePerfCounter(isEnablePerfCounter())
        .falconPerfCounterTags(getFalconPerfCounterTags())
        .falconPushInterval(getFalconPushInterval());
    return builder;
  }

  /**
   * The list of meta server addresses, separated by commas.
   *
   * @return the list of meta server addresses.
   */
  public String getMetaServers() {
    return metaServers;
  }

  /**
   * The timeout for failing to finish an operation. Defaults to {@literal 1000ms}.
   *
   * @return the timeout for failing to finish an operation.
   */
  public Duration getOperationTimeout() {
    return operationTimeout;
  }

  /**
   * The number of background worker threads. Internally it is the number of Netty NIO threads for
   * handling RPC events between client and Replica Servers. Defaults to {@literal 4}.
   *
   * @return The number of background worker threads.
   */
  public int getAsyncWorkers() {
    return asyncWorkers;
  }

  /**
   * Whether to enable performance statistics. If true, the client will periodically report metrics
   * to local falcon agent (currently we only support falcon as monitoring system). Defaults to
   * {@literal false}.
   *
   * @return whether to enable performance statistics.
   */
  public boolean isEnablePerfCounter() {
    return enablePerfCounter;
  }

  /**
   * Additional tags for falcon metrics. Defaults to empty string.
   *
   * @return additional tags for falcon metrics.
   */
  public String getFalconPerfCounterTags() {
    return falconPerfCounterTags;
  }

  /**
   * The interval to report metrics to local falcon agent. Defaults to {@literal 10s}.
   *
   * @return the interval to report metrics to local falcon agent.
   */
  public Duration getFalconPushInterval() {
    return falconPushInterval;
  }
}
