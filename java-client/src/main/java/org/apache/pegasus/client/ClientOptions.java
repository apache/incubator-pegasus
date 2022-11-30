/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.pegasus.client;

import static org.apache.pegasus.client.PConfigUtil.loadConfiguration;

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import org.apache.pegasus.security.Credential;
import org.apache.pegasus.tools.WriteLimiter;
import org.apache.pegasus.util.PropertyUtils;

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
 *     ClientOptions.builder().metaServers("127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603")
 *         .operationTimeout(Duration.ofMillis(1000)).asyncWorkers(4).enablePerfCounter(false)
 *         .falconPerfCounterTags("").falconPushInterval(Duration.ofSeconds(10))
 *         .metaQueryTimeout(Duration.ofMillis(5000)).credential(null).build();
 * }</pre>
 */
public class ClientOptions {

  public static final int MIN_SOCK_CONNECT_TIMEOUT = 1000;

  public static final String PEGASUS_META_SERVERS_KEY = "meta_servers";
  public static final String PEGASUS_OPERATION_TIMEOUT_KEY = "operation_timeout";
  public static final String PEGASUS_ASYNC_WORKERS_KEY = "async_workers";
  public static final String PEGASUS_ENABLE_PERF_COUNTER_KEY = "enable_perf_counter";
  public static final String PEGASUS_PERF_COUNTER_TAGS_KEY = "perf_counter_tags";
  public static final String PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY = "push_counter_interval_secs";
  public static final String PEGASUS_META_QUERY_TIMEOUT_KEY = "meta_query_timeout";
  public static final String PEGASUS_AUTH_PROTOCOL_KEY = "auth_protocol";
  public static final String PEGASUS_SESSION_RESET_TIME_WINDOW_SECS_KEY =
      "session_reset_time_window_secs";

  public static final String DEFAULT_META_SERVERS =
      "127.0.0.1:34601,127.0.0.1:34602,127.0.0.1:34603";
  public static final Duration DEFAULT_OPERATION_TIMEOUT = Duration.ofMillis(1000);
  public static final int DEFAULT_ASYNC_WORKERS = 4;
  public static final boolean DEFAULT_ENABLE_PERF_COUNTER = true;
  public static final String DEFAULT_FALCON_PERF_COUNTER_TAGS = "";
  public static final Duration DEFAULT_FALCON_PUSH_INTERVAL = Duration.ofSeconds(10);
  public static final boolean DEFAULT_ENABLE_WRITE_LIMIT = true;
  public static final Duration DEFAULT_META_QUERY_TIMEOUT = Duration.ofMillis(5000);
  public static final String DEFAULT_AUTH_PROTOCOL = "";
  public static final long DEFAULT_SESSION_RESET_SECS_WINDOW = 30;

  private final String metaServers;
  private final Duration operationTimeout;
  private final int asyncWorkers;
  private final boolean enablePerfCounter;
  private final String falconPerfCounterTags;
  private final Duration falconPushInterval;
  private final boolean enableWriteLimit;
  private final Duration metaQueryTimeout;
  private final Credential credential;
  private final long sessionResetTimeWindowSecs;

  protected ClientOptions(Builder builder) {
    this.metaServers = builder.metaServers;
    this.operationTimeout = builder.operationTimeout;
    this.asyncWorkers = builder.asyncWorkers;
    this.enablePerfCounter = builder.enablePerfCounter;
    this.falconPerfCounterTags = builder.falconPerfCounterTags;
    this.falconPushInterval = builder.falconPushInterval;
    this.enableWriteLimit = builder.enableWriteLimit;
    this.metaQueryTimeout = builder.metaQueryTimeout;
    this.credential = builder.credential;
    this.sessionResetTimeWindowSecs = builder.sessionResetTimeWindowSecs;
  }

  protected ClientOptions(ClientOptions original) {
    this.metaServers = original.getMetaServers();
    this.operationTimeout = original.getOperationTimeout();
    this.asyncWorkers = original.getAsyncWorkers();
    this.enablePerfCounter = original.isEnablePerfCounter();
    this.falconPerfCounterTags = original.getFalconPerfCounterTags();
    this.falconPushInterval = original.getFalconPushInterval();
    this.enableWriteLimit = original.isWriteLimitEnabled();
    this.metaQueryTimeout = original.getMetaQueryTimeout();
    this.credential = original.getCredential();
    this.sessionResetTimeWindowSecs = original.getSessionResetTimeWindowSecs();
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

  public static ClientOptions create(String configPath) throws PException {
    return create(loadConfiguration(configPath));
  }

  public static ClientOptions create(Properties config) throws PException {
    String metaList = config.getProperty(PEGASUS_META_SERVERS_KEY);
    if (metaList == null) {
      throw new IllegalArgumentException("no property set: " + PEGASUS_META_SERVERS_KEY);
    }
    metaList = metaList.trim();
    if (metaList.isEmpty()) {
      throw new IllegalArgumentException("invalid property: " + PEGASUS_META_SERVERS_KEY);
    }

    int asyncWorkers =
        PropertyUtils.getInt(config, PEGASUS_ASYNC_WORKERS_KEY, DEFAULT_ASYNC_WORKERS);
    boolean enablePerfCounter =
        PropertyUtils.getBoolean(
            config, PEGASUS_ENABLE_PERF_COUNTER_KEY, DEFAULT_ENABLE_PERF_COUNTER);
    String perfCounterTags =
        enablePerfCounter
            ? config.getProperty(PEGASUS_PERF_COUNTER_TAGS_KEY, DEFAULT_FALCON_PERF_COUNTER_TAGS)
            : null;
    Duration pushIntervalSecs =
        Duration.ofSeconds(
            PropertyUtils.getLong(
                config,
                PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY,
                DEFAULT_FALCON_PUSH_INTERVAL.getSeconds()));
    Duration operationTimeOut =
        Duration.ofMillis(
            PropertyUtils.getLong(
                config, PEGASUS_OPERATION_TIMEOUT_KEY, DEFAULT_OPERATION_TIMEOUT.toMillis()));
    Duration metaQueryTimeout =
        Duration.ofMillis(
            PropertyUtils.getLong(
                config, PEGASUS_META_QUERY_TIMEOUT_KEY, DEFAULT_META_QUERY_TIMEOUT.toMillis()));
    String authProtocol = config.getProperty(PEGASUS_AUTH_PROTOCOL_KEY, DEFAULT_AUTH_PROTOCOL);
    Credential credential = Credential.create(authProtocol, config);
    long sessionResetTimeWindowSecs =
        PropertyUtils.getLong(
            config, PEGASUS_SESSION_RESET_TIME_WINDOW_SECS_KEY, DEFAULT_SESSION_RESET_SECS_WINDOW);

    return ClientOptions.builder()
        .metaServers(metaList)
        .operationTimeout(operationTimeOut)
        .asyncWorkers(asyncWorkers)
        .enablePerfCounter(enablePerfCounter)
        .falconPerfCounterTags(perfCounterTags)
        .falconPushInterval(pushIntervalSecs)
        .metaQueryTimeout(metaQueryTimeout)
        .credential(credential)
        .sessionResetTimeWindowSecs(sessionResetTimeWindowSecs)
        .build();
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
          && this.falconPushInterval.toMillis() == clientOptions.falconPushInterval.toMillis()
          && this.enableWriteLimit == clientOptions.enableWriteLimit
          && this.metaQueryTimeout.toMillis() == clientOptions.metaQueryTimeout.toMillis()
          && this.credential == clientOptions.credential
          && this.sessionResetTimeWindowSecs == clientOptions.sessionResetTimeWindowSecs;
    }
    return false;
  }

  @Override
  public int hashCode() {
    int result = 20;
    result = 31 * result + metaServers.hashCode();
    result = 31 * result + Objects.hashCode(operationTimeout.toMillis());
    result = 31 * result + asyncWorkers;
    result = 31 * result + (enablePerfCounter ? 1 : 0);
    result = 31 * result + falconPerfCounterTags.hashCode();
    result = 31 * result + Objects.hashCode(falconPushInterval.toMillis());
    result = 31 * result + (enableWriteLimit ? 1 : 0);
    result = 31 * result + Objects.hashCode(metaQueryTimeout.toMillis());
    result = 31 * result + credential.hashCode();
    result = 31 * result + Objects.hashCode(sessionResetTimeWindowSecs);
    return result;
  }

  @Override
  public String toString() {
    String res =
        "ClientOptions{"
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
            + ",enableWriteLimit="
            + enableWriteLimit
            + ", metaQueryTimeout(ms)="
            + metaQueryTimeout.toMillis()
            + ", sessionResetTimeWindowSecs="
            + sessionResetTimeWindowSecs;
    if (credential != null) {
      res += ", credential=" + credential.toString();
    }
    return res + '}';
  }

  /** Builder for {@link ClientOptions}. */
  public static class Builder {
    private String metaServers = DEFAULT_META_SERVERS;
    private Duration operationTimeout = DEFAULT_OPERATION_TIMEOUT;
    private int asyncWorkers = DEFAULT_ASYNC_WORKERS;
    private boolean enablePerfCounter = DEFAULT_ENABLE_PERF_COUNTER;
    private String falconPerfCounterTags = DEFAULT_FALCON_PERF_COUNTER_TAGS;
    private Duration falconPushInterval = DEFAULT_FALCON_PUSH_INTERVAL;
    private boolean enableWriteLimit = DEFAULT_ENABLE_WRITE_LIMIT;
    private Duration metaQueryTimeout = DEFAULT_META_QUERY_TIMEOUT;
    private Credential credential = null;
    private long sessionResetTimeWindowSecs = DEFAULT_SESSION_RESET_SECS_WINDOW;

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
     * The timeout for failing to finish an operation, must be positive. Defaults to {@literal
     * 1000ms}, see {@link #DEFAULT_OPERATION_TIMEOUT}.
     *
     * @param operationTimeout operationTimeout
     * @return {@code this}
     */
    public Builder operationTimeout(Duration operationTimeout) {
      validatePositiveNum(operationTimeout.toMillis());
      this.operationTimeout = operationTimeout;
      return this;
    }

    /**
     * The number of background worker threads, must be positive. Internally it is the number of
     * Netty NIO threads for handling RPC events between client and Replica Servers. Defaults to
     * {@literal 4}, see {@link #DEFAULT_ASYNC_WORKERS}.
     *
     * @param asyncWorkers asyncWorkers thread number
     * @return {@code this}
     */
    public Builder asyncWorkers(int asyncWorkers) {
      validatePositiveNum(asyncWorkers);
      this.asyncWorkers = asyncWorkers;
      return this;
    }

    /**
     * Whether to enable performance statistics. If true, the client will periodically report
     * metrics to local falcon agent (currently we only support falcon as monitoring system).
     * Defaults to {@literal true}, see {@link #DEFAULT_ENABLE_PERF_COUNTER}.
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
     * The interval to report metrics to local falcon agent, must be positive. Defaults to {@literal
     * 10s}, see {@link #DEFAULT_FALCON_PUSH_INTERVAL}.
     *
     * @param falconPushInterval falconPushInterval
     * @return {@code this}
     */
    public Builder falconPushInterval(Duration falconPushInterval) {
      validatePositiveNum(falconPushInterval.toMillis());
      this.falconPushInterval = falconPushInterval;
      return this;
    }

    /**
     * whether to enable write limit . if true, exceed the threshold set will throw exception, See
     * {@linkplain WriteLimiter WriteLimiter}. Defaults to {@literal true}, see {@link
     * #DEFAULT_ENABLE_WRITE_LIMIT}
     *
     * @param enableWriteLimit enableWriteLimit
     * @return {@code this}
     */
    public Builder enableWriteLimit(boolean enableWriteLimit) {
      this.enableWriteLimit = enableWriteLimit;
      return this;
    }

    /**
     * The timeout for query meta server, must be positive. Defaults to {@literal 5000ms}, see
     * {@link #DEFAULT_META_QUERY_TIMEOUT}.
     *
     * @param metaQueryTimeout metaQueryTimeout
     * @return {@code this}
     */
    public Builder metaQueryTimeout(Duration metaQueryTimeout) {
      validatePositiveNum(metaQueryTimeout.toMillis());
      this.metaQueryTimeout = metaQueryTimeout;
      return this;
    }

    /**
     * credential info. Defaults to {@literal null}
     *
     * @param credential credential
     * @return {@code this}
     */
    public Builder credential(Credential credential) {
      this.credential = credential;
      return this;
    }

    /**
     * session reset time window, If the timeout duration exceeds this value, the connection will be
     * reset
     *
     * @param sessionResetTimeWindowSecs sessionResetTimeWindowSecs must >= 10s, Defaults to
     *     {@linkplain #DEFAULT_SESSION_RESET_SECS_WINDOW}
     * @return {@code this}
     */
    public Builder sessionResetTimeWindowSecs(long sessionResetTimeWindowSecs) {
      assert sessionResetTimeWindowSecs >= 10 : "sessionResetTimeWindowSecs must be >= 10s";
      this.sessionResetTimeWindowSecs = sessionResetTimeWindowSecs;
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

    private static void validatePositiveNum(long value) {
      assert value > 0 : String.format("must pass positive value: %d", value);
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
        .falconPushInterval(getFalconPushInterval())
        .enableWriteLimit(isWriteLimitEnabled())
        .metaQueryTimeout(getMetaQueryTimeout())
        .credential(getCredential());
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
   * {@literal true}.
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

  /**
   * whether to enable write limit. if true, exceed the threshold set will throw exception, See
   * {@linkplain WriteLimiter WriteLimiter}. Defaults to {@literal true}, See {@link
   * #DEFAULT_ENABLE_WRITE_LIMIT}
   *
   * @return whether to enable write size limit
   */
  public boolean isWriteLimitEnabled() {
    return enableWriteLimit;
  }

  /**
   * The timeout for query meta server. Defaults to {@literal 5000ms}.
   *
   * @return the timeout for query meta server.
   */
  public Duration getMetaQueryTimeout() {
    return metaQueryTimeout;
  }

  /**
   * credential info. Defaults to {@literal null}
   *
   * @return credential
   */
  public Credential getCredential() {
    return credential;
  }

  /**
   * session reset time window, If the timeout duration exceeds this value, the connection will be
   * reset
   *
   * @return sessionResetTimeWindowSecs
   */
  public long getSessionResetTimeWindowSecs() {
    return sessionResetTimeWindowSecs;
  }
}
