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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;

import java.nio.charset.Charset;
import javax.security.auth.Subject;
import org.apache.pegasus.apps.negotiation_response;
import org.apache.pegasus.apps.negotiation_status;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.rpc.async.ReplicaSession;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class NegotiationTest {
  private Negotiation negotiation = new Negotiation(null, new Subject(), "", "");

  @Test
  public void testStart() {
    Negotiation mockNegotiation = Mockito.spy(negotiation);

    Mockito.doNothing().when(mockNegotiation).send(any(), any());
    mockNegotiation.start();
    assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_LIST_MECHANISMS);
  }

  @Test
  public void tetGetMatchMechanism() {
    String matchMechanism = negotiation.getMatchMechanism("GSSAPI,ABC");
    assertEquals(matchMechanism, "GSSAPI");

    matchMechanism = negotiation.getMatchMechanism("TEST,ABC");
    assertEquals(matchMechanism, "");
  }

  @Test
  public void testCheckStatus() {
    negotiation_status expectedStatus = negotiation_status.SASL_LIST_MECHANISMS;

    assertDoesNotThrow(
        () -> negotiation.checkStatus(negotiation_status.SASL_LIST_MECHANISMS, expectedStatus));

    assertThrows(
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
    assertDoesNotThrow(
        () -> {
          Mockito.when(mockNegotiation.saslWrapper.init(any())).thenReturn(new byte[0]);
        });

    // normal case
    assertDoesNotThrow(
        () -> {
          negotiation_response response =
              new negotiation_response(
                  negotiation_status.SASL_LIST_MECHANISMS_RESP,
                  new blob("GSSAPI".getBytes(Charset.defaultCharset())));
          mockNegotiation.onRecvMechanisms(response);
          assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_SELECT_MECHANISMS);
        });

    // deal with wrong response.msg
    assertThrows(
        Exception.class,
        () -> {
          negotiation_response response =
              new negotiation_response(
                  negotiation_status.SASL_LIST_MECHANISMS,
                  new blob("NOTSUPPORTED".getBytes(Charset.defaultCharset())));
          mockNegotiation.onRecvMechanisms(response);
        });

    // deal with wrong response.status
    assertThrows(
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
      fail();
    }

    // normal case
    negotiation_response response =
        new negotiation_response(
            negotiation_status.SASL_SELECT_MECHANISMS_RESP, new blob(new byte[0]));
    assertDoesNotThrow(() -> mockNegotiation.onMechanismSelected(response));
    assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_INITIATE);

    // deal with wrong response.status
    response.status = negotiation_status.SASL_LIST_MECHANISMS;
    assertThrows(Exception.class, () -> mockNegotiation.onMechanismSelected(response));
  }

  @Test
  public void testChallenge() {
    Negotiation mockNegotiation = Mockito.spy(negotiation);
    SaslWrapper mockSaslWrapper = Mockito.mock(SaslWrapper.class);
    ReplicaSession mockSession = Mockito.mock(ReplicaSession.class);
    mockNegotiation.saslWrapper = mockSaslWrapper;
    mockNegotiation.session = mockSession;

    // mock operation
    Mockito.doNothing().when(mockNegotiation).send(any(), any());
    Mockito.doNothing().when(mockNegotiation.session).onAuthSucceed();
    try {
      Mockito.when(mockNegotiation.saslWrapper.evaluateChallenge(any())).thenReturn(new blob());
    } catch (Exception ex) {
      fail();
    }

    // normal case
    assertDoesNotThrow(
        () -> {
          negotiation_response response =
              new negotiation_response(negotiation_status.SASL_CHALLENGE, new blob(new byte[0]));
          mockNegotiation.onChallenge(response);
          assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_CHALLENGE_RESP);

          response = new negotiation_response(negotiation_status.SASL_SUCC, new blob(new byte[0]));
          mockNegotiation.onChallenge(response);
          assertEquals(mockNegotiation.getStatus(), negotiation_status.SASL_SUCC);
        });

    // deal with wrong response.status
    assertThrows(
        Exception.class,
        () -> {
          negotiation_response response =
              new negotiation_response(
                  negotiation_status.SASL_LIST_MECHANISMS, new blob(new byte[0]));
          mockNegotiation.onChallenge(response);
        });
  }
}
