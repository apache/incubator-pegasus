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
package com.xiaomi.infra.pegasus.security;

import com.sun.security.auth.callback.TextCallbackHandler;
import com.xiaomi.infra.pegasus.rpc.async.ReplicaSession;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class KerberosProtocol implements AuthProtocol {
  private static final Logger logger = LoggerFactory.getLogger(KerberosProtocol.class);

  // Subject is a JAAS internal class, Ref:
  // https://docs.oracle.com/javase/7/docs/technotes/guides/security/jaas/JAASRefGuide.html
  //
  // To authorize access to resources, applications first need to authenticate the source of the
  // request. The JAAS framework defines the term "subject" to represent the source of a request. A
  // subject may be any entity, such as a person or a service.
  private Subject subject;
  // The LoginContext class provides the basic methods used to authenticate subjects, and provides a
  // way to develop an application independent of the underlying authentication technology
  private LoginContext loginContext;
  private String serviceName;
  private String serviceFqdn;

  KerberosProtocol(String serviceName, String serviceFqdn, String keyTab, String principal)
      throws IllegalArgumentException {
    this.serviceName = serviceName;
    this.serviceFqdn = serviceFqdn;
    try {
      // Authenticate the Subject (the source of the request)
      // A LoginModule uses a CallbackHandler to communicate with the user to obtain authentication
      // information
      this.subject = new Subject();
      this.loginContext =
          new LoginContext(
              "pegasus-client",
              subject,
              new TextCallbackHandler(),
              getLoginContextConfiguration(keyTab, principal));
      this.loginContext.login();
    } catch (LoginException le) {
      throw new IllegalArgumentException("login failed: ", le);
    }

    logger.info("login succeed, as user {}", subject.getPrincipals().toString());
  }

  @Override
  public void authenticate(ReplicaSession session) {
    Negotiation negotiation = new Negotiation(session, subject, serviceName, serviceFqdn);
    negotiation.start();
  }

  private static Configuration getLoginContextConfiguration(String keyTab, String principal) {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        Map<String, String> options = new HashMap<>();
        // TGT is obtained from the ticket cache.
        options.put("useTicketCache", "true");
        // get the principal's key from the the keytab
        options.put("useKeyTab", "true");
        // renew the TGT
        options.put("renewTGT", "true");
        // keytab or the principal's key to be stored in the Subject's private credentials.
        options.put("storeKey", "true");
        // the file name of the keytab to get principal's secret key.
        options.put("keyTab", keyTab);
        // the name of the principal that should be used
        options.put("principal", principal);

        return new AppConfigurationEntry[] {
          new AppConfigurationEntry(
              "com.sun.security.auth.module.Krb5LoginModule",
              AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
              options)
        };
      }
    };
  }
}
