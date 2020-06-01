// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import java.util.Properties;

/** ClusterOptions is the internal options for connecting a Pegasus cluster. */
public class ClusterOptions {
  public static final int MIN_SOCK_CONNECT_TIMEOUT = 1000;

  public static final String PEGASUS_META_SERVERS_KEY = "meta_servers";

  public static final String PEGASUS_OPERATION_TIMEOUT_KEY = "operation_timeout";
  public static final String PEGASUS_OPERATION_TIMEOUT_DEF = "1000";

  public static final String PEGASUS_ASYNC_WORKERS_KEY = "async_workers";
  public static final String PEGASUS_ASYNC_WORKERS_DEF =
      String.valueOf(Runtime.getRuntime().availableProcessors());

  public static final String PEGASUS_ENABLE_PERF_COUNTER_KEY = "enable_perf_counter";
  public static final String PEGASUS_ENABLE_PERF_COUNTER_DEF = "true";

  public static final String PEGASUS_PERF_COUNTER_TAGS_KEY = "perf_counter_tags";
  public static final String PEGASUS_PERF_COUNTER_TAGS_DEF = "";

  public static final String PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY = "push_counter_interval_secs";
  public static final String PEGASUS_PUSH_COUNTER_INTERVAL_SECS_DEF = "60";

  public static final String PEGASUS_META_QUERY_TIMEOUT_KEY = "meta_query_timeout";
  public static final String PEGASUS_META_QUERY_TIMEOUT_DEF = "5000";

  public static String[] allKeys() {
    return new String[] {
      PEGASUS_META_SERVERS_KEY,
      PEGASUS_OPERATION_TIMEOUT_KEY,
      PEGASUS_ASYNC_WORKERS_KEY,
      PEGASUS_ENABLE_PERF_COUNTER_KEY,
      PEGASUS_PERF_COUNTER_TAGS_KEY,
      PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY,
      PEGASUS_META_QUERY_TIMEOUT_KEY
    };
  }

  private final int operationTimeout;
  private final String[] metaList;
  private final int asyncWorkers;
  private final boolean enablePerfCounter;
  private final String perfCounterTags;
  private final int pushCounterIntervalSecs;
  private final int metaQueryTimeout;

  public int operationTimeout() {
    return this.operationTimeout;
  }

  public String[] metaList() {
    return this.metaList == null ? null : this.metaList.clone();
  }

  public int asyncWorkers() {
    return this.asyncWorkers;
  }

  public boolean enablePerfCounter() {
    return this.enablePerfCounter;
  }

  public String perfCounterTags() {
    return this.perfCounterTags;
  }

  public int pushCounterIntervalSecs() {
    return this.pushCounterIntervalSecs;
  }

  public int metaQueryTimeout() {
    return this.metaQueryTimeout;
  }

  public static ClusterOptions create(Properties config) {
    int operationTimeout =
        Integer.parseInt(
            config.getProperty(PEGASUS_OPERATION_TIMEOUT_KEY, PEGASUS_OPERATION_TIMEOUT_DEF));
    String metaList = config.getProperty(PEGASUS_META_SERVERS_KEY);
    if (metaList == null) {
      throw new IllegalArgumentException("no property set: " + PEGASUS_META_SERVERS_KEY);
    }
    metaList = metaList.trim();
    if (metaList.isEmpty()) {
      throw new IllegalArgumentException("invalid property: " + PEGASUS_META_SERVERS_KEY);
    }
    String[] address = metaList.split(",");

    int asyncWorkers =
        Integer.parseInt(config.getProperty(PEGASUS_ASYNC_WORKERS_KEY, PEGASUS_ASYNC_WORKERS_DEF));
    boolean enablePerfCounter =
        Boolean.parseBoolean(
            config.getProperty(PEGASUS_ENABLE_PERF_COUNTER_KEY, PEGASUS_ENABLE_PERF_COUNTER_DEF));
    String perfCounterTags =
        enablePerfCounter
            ? config.getProperty(PEGASUS_PERF_COUNTER_TAGS_KEY, PEGASUS_PERF_COUNTER_TAGS_DEF)
            : null;
    int pushIntervalSecs =
        Integer.parseInt(
            config.getProperty(
                PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY, PEGASUS_PUSH_COUNTER_INTERVAL_SECS_DEF));
    int metaQueryTimeout =
        Integer.parseInt(
            config.getProperty(PEGASUS_META_QUERY_TIMEOUT_KEY, PEGASUS_META_QUERY_TIMEOUT_DEF));

    return new ClusterOptions(
        operationTimeout,
        address,
        asyncWorkers,
        enablePerfCounter,
        perfCounterTags,
        pushIntervalSecs,
        metaQueryTimeout);
  }

  public static ClusterOptions forTest(String[] metaList) {
    return new ClusterOptions(1000, metaList, 1, false, null, 60, 1000);
  }

  private ClusterOptions(
      int operationTimeout,
      String[] metaList,
      int asyncWorkers,
      boolean enablePerfCounter,
      String perfCounterTags,
      int pushCounterIntervalSecs,
      int metaQueryTimeout) {
    this.operationTimeout = operationTimeout;
    this.metaList = metaList;
    this.asyncWorkers = asyncWorkers;
    this.enablePerfCounter = enablePerfCounter;
    this.perfCounterTags = perfCounterTags;
    this.pushCounterIntervalSecs = pushCounterIntervalSecs;
    this.metaQueryTimeout = metaQueryTimeout;
  }
}
