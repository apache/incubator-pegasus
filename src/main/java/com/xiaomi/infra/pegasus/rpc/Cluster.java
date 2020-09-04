// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.rpc.async.ClusterManager;
import org.apache.thrift.TException;

public abstract class Cluster {

  public static Cluster createCluster(ClientOptions clientOptions)
      throws IllegalArgumentException, PException {
    return new ClusterManager(clientOptions);
  }

  public abstract String[] getMetaList();

  public abstract Table openTable(String name, InternalTableOptions options)
      throws ReplicationException, TException;

  public abstract void close();
}
