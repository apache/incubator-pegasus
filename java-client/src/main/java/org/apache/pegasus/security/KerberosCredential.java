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

import java.util.Properties;

/**
 * kerberos credential information
 *
 * <p>To create a new instance with Configuration:
 *
 * <pre>{@code
 * new KerberosCredential(config);
 * }</pre>
 *
 * To customize the settings:
 *
 * <pre>{@code
 * KerberosCredential credential =
 *      KerberosCredential.Builder()
 *          .serviceName("")
 *          .serviceFqdn("")
 *          .keytab("")
 *          .principal("")
 *          .build();
 * }</pre>
 */
class KerberosCredential implements Credential {
  public static final String PEGASUS_SERVICE_NAME_KEY = "kerberos_service_name";
  public static final String PEGASUS_SERVICE_FQDN_KEY = "kerberos_service_fqdn";
  public static final String PEGASUS_KEYTAB_KEY = "kerberos_keytab";
  public static final String PEGASUS_PRINCIPAL_KEY = "kerberos_principal";

  public static final String DEFAULT_SERVICE_NAME = "";
  public static final String DEFAULT_SERVICE_FQDN = "";
  public static final String DEFAULT_KEYTAB = "";
  public static final String DEFAULT_PRINCIPAL = "";

  private String serviceName;
  private String serviceFqdn;
  private String keyTab;
  private String principal;

  KerberosCredential(Properties config) {
    this.serviceName = config.getProperty(PEGASUS_SERVICE_NAME_KEY, DEFAULT_SERVICE_NAME);
    this.serviceFqdn = config.getProperty(PEGASUS_SERVICE_FQDN_KEY, DEFAULT_SERVICE_FQDN);
    this.keyTab = config.getProperty(PEGASUS_KEYTAB_KEY, DEFAULT_KEYTAB);
    this.principal = config.getProperty(PEGASUS_PRINCIPAL_KEY, DEFAULT_PRINCIPAL);
  }

  KerberosCredential(Builder builder) {
    this.serviceName = builder.serviceName;
    this.serviceFqdn = builder.serviceFqdn;
    this.keyTab = builder.keyTab;
    this.principal = builder.principal;
  }

  @Override
  public AuthProtocol getProtocol() {
    return new KerberosProtocol(serviceName, serviceFqdn, keyTab, principal);
  }

  @Override
  public String toString() {
    return "KerberosCredential{"
        + "serviceName='"
        + serviceName
        + '\''
        + ", serviceFqdn='"
        + serviceFqdn
        + '\''
        + ", keyTab='"
        + keyTab
        + '\''
        + ", principal='"
        + principal
        + '\''
        + '}';
  }

  public static class Builder {
    private String serviceName;
    private String serviceFqdn;
    private String keyTab;
    private String principal;

    /**
     * kerberos service name. Defaults to {@literal ""}, see {@link #DEFAULT_SERVICE_NAME}
     *
     * @param serviceName
     * @return {@code this}
     */
    public Builder serviceName(String serviceName) {
      this.serviceName = serviceName;
      return this;
    }

    /**
     * kerberos service fqdn. Defaults to {@literal ""}, see {@link #DEFAULT_SERVICE_FQDN}
     *
     * @param serviceFqdn
     * @return {@code this}
     */
    public Builder serviceFqdn(String serviceFqdn) {
      this.serviceFqdn = serviceFqdn;
      return this;
    }

    /**
     * kerberos keytab file path. Defaults to {@literal ""}, see {@link #DEFAULT_KEYTAB}.
     *
     * @param keyTab
     * @return {@code this}
     */
    public Builder keyTab(String keyTab) {
      this.keyTab = keyTab;
      return this;
    }

    /**
     * kerberos principal. Defaults to {@literal ""}, see {@link #DEFAULT_PRINCIPAL}.
     *
     * @param principal
     * @return {@code this}
     */
    public Builder principal(String principal) {
      this.principal = principal;
      return this;
    }

    /**
     * Create a new instance of {@link KerberosCredential}.
     *
     * @return new instance of {@link KerberosCredential}.
     */
    public KerberosCredential build() {
      return new KerberosCredential(this);
    }
  }
}
