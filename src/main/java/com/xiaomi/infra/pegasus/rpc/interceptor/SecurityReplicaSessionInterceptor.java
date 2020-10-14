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
package com.xiaomi.infra.pegasus.rpc.interceptor;

import com.sun.security.auth.callback.TextCallbackHandler;
import com.xiaomi.infra.pegasus.rpc.async.Negotiation;
import com.xiaomi.infra.pegasus.rpc.async.ReplicaSession;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.slf4j.Logger;

public class SecurityReplicaSessionInterceptor implements ReplicaSessionInterceptor {
  private static final Logger logger =
      org.slf4j.LoggerFactory.getLogger(SecurityReplicaSessionInterceptor.class);

  private String serviceName;
  private String serviceFqdn;
  private Subject subject;
  private LoginContext loginContext;

  public SecurityReplicaSessionInterceptor(String serviceName, String serviceFqdn)
      throws IllegalArgumentException {
    this.serviceName = serviceName;
    this.serviceFqdn = serviceFqdn;

    try {
      loginContext = new LoginContext("client", new TextCallbackHandler());
      loginContext.login();

      subject = loginContext.getSubject();
      if (subject == null) {
        throw new LoginException("subject is null");
      }
    } catch (LoginException le) {
      throw new IllegalArgumentException("login failed", le);
    }

    logger.info("login succeed, as user {}", subject.getPrincipals().toString());
  }

  public void onConnected(ReplicaSession session) {
    Negotiation negotiation = new Negotiation(session, subject, serviceName, serviceFqdn);
    negotiation.start();
  }
}
