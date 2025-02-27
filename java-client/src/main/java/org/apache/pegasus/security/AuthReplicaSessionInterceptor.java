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
package org.apache.pegasus.security;

import java.io.Closeable;
import org.apache.pegasus.client.ClientOptions;
import org.apache.pegasus.rpc.async.ReplicaSession;
import org.apache.pegasus.rpc.interceptor.ReplicaSessionInterceptor;

public class AuthReplicaSessionInterceptor implements ReplicaSessionInterceptor, Closeable {
  private final AuthProtocol protocol;

  public AuthReplicaSessionInterceptor(ClientOptions options) throws IllegalArgumentException {
    this.protocol = options.getCredential().getProtocol();
  }

  @Override
  public void onConnected(ReplicaSession session) {
    protocol.authenticate(session);
  }

  @Override
  public boolean onSendMessage(ReplicaSession session, final ReplicaSession.RequestEntry entry) {
    // tryPendRequest returns false means that the negotiation is successful now.
    return protocol.isAuthRequest(entry) || !session.tryPendRequest(entry);
  }

  @Override
  public void close() {
    protocol.close();
  }
}
