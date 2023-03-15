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

import com.sun.security.auth.callback.TextCallbackHandler;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import javax.security.auth.kerberos.KerberosTicket;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import org.apache.commons.lang3.StringUtils;
import org.apache.pegasus.operator.negotiation_operator;
import org.apache.pegasus.rpc.async.ReplicaSession;
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
  private String serviceName;
  private String serviceFqdn;
  private String keyTab;
  private String principal;
  final int CHECK_TGT_INTEVAL_SECONDS = 10;
  final ScheduledExecutorService service =
      Executors.newSingleThreadScheduledExecutor(
          new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r);
              t.setDaemon(true);
              t.setName("TGT renew for pegasus");
              return t;
            }
          });

  KerberosProtocol(String serviceName, String serviceFqdn, String keyTab, String principal)
      throws IllegalArgumentException {
    this.serviceName = serviceName;
    this.serviceFqdn = serviceFqdn;
    this.keyTab = keyTab;
    this.principal = principal;

    this.login();
    scheduleCheckTGTAndRelogin();
    logger.info("login succeed, as user {}", subject.getPrincipals().toString());
  }

  @Override
  public String name() {
    return "kerberos";
  }

  @Override
  public void authenticate(ReplicaSession session) {
    Negotiation negotiation = new Negotiation(session, subject, serviceName, serviceFqdn);
    negotiation.start();
  }

  @Override
  public boolean isAuthRequest(final ReplicaSession.RequestEntry entry) {
    return entry.op instanceof negotiation_operator;
  }

  private void scheduleCheckTGTAndRelogin() {
    service.scheduleAtFixedRate(
        () -> checkTGTAndRelogin(),
        CHECK_TGT_INTEVAL_SECONDS,
        CHECK_TGT_INTEVAL_SECONDS,
        TimeUnit.SECONDS);
  }

  private void checkTGTAndRelogin() {
    KerberosTicket tgt = getTGT();
    if (tgt != null && System.currentTimeMillis() < getRefreshTime(tgt)) {
      return;
    }

    // relogin if tgt is null or the tgt is almost expired
    this.login();
  }

  // The default refresh window is 0.8. e.g. if the lifetime of a tgt is 5min, the tgt will be
  // refreshed after 4 min(5min * 0.8 = 4min) passed
  private long getRefreshTime(KerberosTicket tgt) {
    long start = tgt.getStartTime().getTime();
    long end = tgt.getEndTime().getTime();
    return start + (long) ((float) (end - start) * 0.8F);
  }

  private KerberosTicket getTGT() {
    Set<KerberosTicket> tickets = this.subject.getPrivateCredentials(KerberosTicket.class);

    KerberosTicket ticket;
    Iterator iter = tickets.iterator();
    do {
      if (!iter.hasNext()) {
        return null;
      }

      ticket = (KerberosTicket) iter.next();
    } while (!isTGSPrincipal(ticket.getServer()));

    return ticket;
  }

  private boolean isTGSPrincipal(KerberosPrincipal principal) {
    if (principal == null) {
      return false;
    } else {
      return principal
          .getName()
          .equals("krbtgt/" + principal.getRealm() + "@" + principal.getRealm());
    }
  }

  private void login() throws IllegalArgumentException {
    try {
      // Authenticate the Subject (the source of the request)
      // A LoginModule uses a CallbackHandler to communicate with the user to obtain authentication
      // information.
      // The LoginContext class provides the basic methods used to authenticate subjects, and
      // provides a way to develop an application independent of the underlying authentication
      // technology
      LoginContext loginContext =
          new LoginContext(
              "pegasus-client",
              null,
              new TextCallbackHandler(),
              getLoginContextConfiguration(keyTab, principal));
      loginContext.login();
      this.subject = loginContext.getSubject();
    } catch (LoginException le) {
      throw new IllegalArgumentException("login failed: ", le);
    }
  }

  private static Configuration getLoginContextConfiguration(String keyTab, String principal) {
    return new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        Map<String, String> options = new HashMap<>();
        if (StringUtils.isBlank(keyTab)) {
          // TGT is obtained from the ticket cache.
          options.put("useTicketCache", "true");
          // renew the TGT
          options.put("renewTGT", "true");
        } else {
          // get the principal's key from the the keytab
          options.put("useKeyTab", "true");
          // keytab or the principal's key to be stored in the Subject's private credentials.
          options.put("storeKey", "true");
          // the file name of the keytab to get principal's secret key.
          options.put("keyTab", keyTab);
        }
        // the name of the principal that should be used
        options.put("principal", principal);
        // try to debug kerberos
        options.put("debug", System.getProperty("sun.security.krb5.debug", "false"));
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
