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

import static org.mockito.ArgumentMatchers.any;

import com.xiaomi.infra.pegasus.apps.negotiation_response;
import com.xiaomi.infra.pegasus.apps.negotiation_status;
import com.xiaomi.infra.pegasus.base.blob;
import java.nio.charset.Charset;
import javax.security.auth.Subject;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.mockito.Mockito;

public class NegotiationTest {
  private Negotiation negotiation = new Negotiation(null, new Subject(), "", "");

  @Test
  public void testStart() {
    Negotiation mockNegotiation = Mockito.spy(negotiation);

    Mockito.doNothing().when(mockNegotiation).send(any(), any());
    mockNegotiation.start();
    Assert.assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_LIST_MECHANISMS);
  }

  @Test
  public void tetGetMatchMechanism() {
    String matchMechanism = negotiation.getMatchMechanism("GSSAPI,ABC");
    Assert.assertEquals(matchMechanism, "GSSAPI");

    matchMechanism = negotiation.getMatchMechanism("TEST,ABC");
    Assert.assertEquals(matchMechanism, "");
  }

  @Test
  public void testCheckStatus() {
    negotiation_status expectedStatus = negotiation_status.SASL_LIST_MECHANISMS;

    Assertions.assertDoesNotThrow(
        () -> negotiation.checkStatus(negotiation_status.SASL_LIST_MECHANISMS, expectedStatus));

    Assertions.assertThrows(
        Exception.class,
        () ->
            negotiation.checkStatus(negotiation_status.SASL_LIST_MECHANISMS_RESP, expectedStatus));
  }

  @Test
  public void testRecvMechanisms() {
    Negotiation mockNegotiation = Mockito.spy(negotiation);
    SaslWrapper mockSaslWrapper = Mockito.mock(SaslWrapper.class);
    mockNegotiation.saslWrapper = mockSaslWrapper;

    Mockito.doNothing().when(mockNegotiation).send(any(), any());
    Assertions.assertDoesNotThrow(
        () -> {
          Mockito.when(mockNegotiation.saslWrapper.init(any())).thenReturn(new byte[0]);
        });

    // normal case
    Assertions.assertDoesNotThrow(
        () -> {
          negotiation_response response =
              new negotiation_response(
                  negotiation_status.SASL_LIST_MECHANISMS_RESP,
                  new blob("GSSAPI".getBytes(Charset.defaultCharset())));
          mockNegotiation.onRecvMechanisms(response);
          Assert.assertEquals(
              mockNegotiation.getStatus(), negotiation_status.SASL_SELECT_MECHANISMS);
        });

    // deal with wrong response.msg
    Assertions.assertThrows(
        Exception.class,
        () -> {
          negotiation_response response =
              new negotiation_response(
                  negotiation_status.SASL_LIST_MECHANISMS,
                  new blob("NOTSUPPORTED".getBytes(Charset.defaultCharset())));
          mockNegotiation.onRecvMechanisms(response);
        });

    // deal with wrong response.status
    Assertions.assertThrows(
        Exception.class,
        () -> {
          negotiation_response response =
              new negotiation_response(
                  negotiation_status.SASL_LIST_MECHANISMS, new blob(new byte[0]));
          mockNegotiation.onRecvMechanisms(response);
        });
  }

  @Test
  public void testMechanismSelected() {
    Negotiation mockNegotiation = Mockito.spy(negotiation);
    SaslWrapper mockSaslWrapper = Mockito.mock(SaslWrapper.class);
    mockNegotiation.saslWrapper = mockSaslWrapper;

    Mockito.doNothing().when(mockNegotiation).send(any(), any());
    try {
      Mockito.when(mockNegotiation.saslWrapper.getInitialResponse()).thenReturn(new blob());
    } catch (Exception ex) {
      Assert.fail();
    }

    // normal case
    negotiation_response response =
        new negotiation_response(
            negotiation_status.SASL_SELECT_MECHANISMS_RESP, new blob(new byte[0]));
    Assertions.assertDoesNotThrow(() -> mockNegotiation.onMechanismSelected(response));
    Assert.assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_INITIATE);

    // deal with wrong response.status
    response.status = negotiation_status.SASL_LIST_MECHANISMS;
    Assertions.assertThrows(Exception.class, () -> mockNegotiation.onMechanismSelected(response));
  }
}
