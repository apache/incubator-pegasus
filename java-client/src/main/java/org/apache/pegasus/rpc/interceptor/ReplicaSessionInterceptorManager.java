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
package org.apache.pegasus.rpc.interceptor;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.rpc.async.ReplicaSession;
import org.apache.pegasus.security.AuthReplicaSessionInterceptor;

public class ReplicaSessionInterceptorManager implements Closeable {
  private List<ReplicaSessionInterceptor> interceptors = new ArrayList<>();

  public ReplicaSessionInterceptorManager(ClientOptions options) {
    if (options.getCredential() != null
        && !options.getCredential().getProtocol().name().isEmpty()) {
      ReplicaSessionInterceptor authInterceptor = new AuthReplicaSessionInterceptor(options);
      interceptors.add(authInterceptor);
    }
  }

  public void onConnected(ReplicaSession session) {
    for (ReplicaSessionInterceptor interceptor : interceptors) {
      interceptor.onConnected(session);
    }
  }

  public boolean onSendMessage(ReplicaSession session, final ReplicaSession.RequestEntry entry) {
    for (ReplicaSessionInterceptor interceptor : interceptors) {
      if (!interceptor.onSendMessage(session, entry)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    for (ReplicaSessionInterceptor interceptor : interceptors) {
      interceptor.close();
    }
  }
}
