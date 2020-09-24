package com.xiaomi.infra.pegasus.rpc.interceptor;

import com.xiaomi.infra.pegasus.rpc.async.ReplicaSession;

public interface ReplicaSessionInterceptor {
  // The behavior when a rpc session is connected.
  void onConnected(ReplicaSession session);
}
