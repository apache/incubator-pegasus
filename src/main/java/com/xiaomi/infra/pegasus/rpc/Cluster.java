// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import com.xiaomi.infra.pegasus.rpc.async.ClusterManager;
import com.xiaomi.infra.pegasus.thrift.TException;
import java.util.Properties;

public abstract class Cluster {
  public static final int MIN_SOCK_CONNECT_TIMEOUT = 1000;

  public static final String PEGASUS_META_SERVERS_KEY = "meta_servers";

  public static final String PEGASUS_OPERATION_TIMEOUT_KEY = "operation_timeout";
  public static final String PEGASUS_OPERATION_TIMEOUT_DEF = "1000";

  public static final String PEGASUS_ASYNC_WORKERS_KEY = "async_workers";
  public static final String PEGASUS_ASYNC_WORKERS_DEF =
      String.valueOf(Runtime.getRuntime().availableProcessors());

  public static final String PEGASUS_ENABLE_PERF_COUNTER_KEY = "enable_perf_counter";
  public static final String PEGASUS_ENABLE_PERF_COUNTER_VALUE = "false";

  public static final String PEGASUS_PERF_COUNTER_TAGS_KEY = "perf_counter_tags";
  public static final String PEGASUS_PERF_COUNTER_TAGS_DEF = "";

  public static final String PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY = "push_counter_interval_secs";
  public static final String PEGASUS_PUSH_COUNTER_INTERVAL_SECS_DEF = "60";

  public static Cluster createCluster(Properties config) throws IllegalArgumentException {
    int operatorTimeout =
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
            config.getProperty(PEGASUS_ENABLE_PERF_COUNTER_KEY, PEGASUS_ENABLE_PERF_COUNTER_VALUE));
    String perfCounterTags =
        enablePerfCounter
            ? config.getProperty(PEGASUS_PERF_COUNTER_TAGS_KEY, PEGASUS_PERF_COUNTER_TAGS_DEF)
            : null;
    int pushIntervalSecs =
        Integer.parseInt(
            config.getProperty(
                PEGASUS_PUSH_COUNTER_INTERVAL_SECS_KEY, PEGASUS_PUSH_COUNTER_INTERVAL_SECS_DEF));
    return new ClusterManager(
        operatorTimeout,
        asyncWorkers,
        enablePerfCounter,
        perfCounterTags,
        pushIntervalSecs,
        address);
  }

  public abstract String[] getMetaList();

  public abstract Table openTable(String name, KeyHasher function)
      throws ReplicationException, TException;

  public abstract void close();
}
