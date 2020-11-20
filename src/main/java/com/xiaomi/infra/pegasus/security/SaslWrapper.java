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

import java.nio.charset.Charset;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;

class SaslWrapper {
  private SaslClient saslClient;
  private Subject subject;
  private String serviceName;
  private String serviceFQDN;
  private HashMap<String, Object> properties = new HashMap<>();

  SaslWrapper(Subject subject, String serviceName, String serviceFQDN) {
    this.subject = subject;
    this.serviceName = serviceName;
    this.serviceFQDN = serviceFQDN;
    this.properties.put(Sasl.QOP, "auth");
  }

  byte[] init(String[] mechanims) throws PrivilegedActionException {
    return Subject.doAs(
        subject,
        (PrivilegedExceptionAction<byte[]>)
            () -> {
              saslClient =
                  Sasl.createSaslClient(
                      mechanims, null, serviceName, serviceFQDN, properties, null);
              return saslClient.getMechanismName().getBytes(Charset.defaultCharset());
            });
  }
}
