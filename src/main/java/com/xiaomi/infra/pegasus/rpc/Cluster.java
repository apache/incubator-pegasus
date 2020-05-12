// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import com.xiaomi.infra.pegasus.rpc.async.ClusterManager;
import java.util.Properties;
import org.apache.thrift.TException;

public abstract class Cluster {

  public static Cluster createCluster(Properties config) throws IllegalArgumentException {
    return new ClusterManager(ClusterOptions.create(config));
  }

  public abstract String[] getMetaList();

  public abstract Table openTable(String name, TableOptions options)
      throws ReplicationException, TException;

  public abstract void close();
}
