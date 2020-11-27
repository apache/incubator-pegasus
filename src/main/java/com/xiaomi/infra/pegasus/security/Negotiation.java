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

import com.xiaomi.infra.pegasus.apps.negotiation_request;
import com.xiaomi.infra.pegasus.apps.negotiation_response;
import com.xiaomi.infra.pegasus.apps.negotiation_status;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.operator.negotiation_operator;
import com.xiaomi.infra.pegasus.rpc.ReplicationException;
import com.xiaomi.infra.pegasus.rpc.async.ReplicaSession;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import javax.security.auth.Subject;
import org.slf4j.Logger;

class Negotiation {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Negotiation.class);
  // Because negotiation message is always the first rpc sent to pegasus server,
  // which will cost much more time. so we set negotiation timeout to 10s here
  private static final int negotiationTimeoutMS = 10000;
  private static final List<String> expectedMechanisms = Collections.singletonList("GSSAPI");

  private negotiation_status status;
  ReplicaSession session;
  SaslWrapper saslWrapper;

  Negotiation(ReplicaSession session, Subject subject, String serviceName, String serviceFQDN) {
    this.saslWrapper = new SaslWrapper(subject, serviceName, serviceFQDN);
    this.session = session;
  }

  void start() {
    status = negotiation_status.SASL_LIST_MECHANISMS;
    send(status, new blob(new byte[0]));
  }

  void send(negotiation_status status, blob msg) {
    negotiation_request request = new negotiation_request(status, msg);
    negotiation_operator operator = new negotiation_operator(request);
    session.asyncSend(
        operator, new RecvHandler(operator), negotiationTimeoutMS, /* isBackupRequest */ false);
  }

  private class RecvHandler implements Runnable {
    private negotiation_operator op;

    RecvHandler(negotiation_operator op) {
      this.op = op;
    }

    @Override
    public void run() {
      try {
        if (op.rpc_error.errno != error_code.error_types.ERR_OK) {
          throw new ReplicationException(op.rpc_error.errno);
        }
        handleResponse();
      } catch (Exception e) {
        logger.error("Negotiation failed", e);
        negotiationFailed();
      }
    }

    private void handleResponse() throws Exception {
      final negotiation_response resp = op.get_response();
      if (resp == null) {
        throw new Exception("RecvHandler received a null response, abandon it");
      }

      switch (status) {
        case SASL_LIST_MECHANISMS:
          onRecvMechanisms(resp);
          break;
        case SASL_SELECT_MECHANISMS:
          onMechanismSelected(resp);
          break;
        case SASL_INITIATE:
        case SASL_CHALLENGE_RESP:
          onChallenge(resp);
          break;
        default:
          throw new Exception("unexpected negotiation status: " + resp.status);
      }
    }
  }

  public void onRecvMechanisms(negotiation_response response) throws Exception {
    checkStatus(response.status, negotiation_status.SASL_LIST_MECHANISMS_RESP);

    String[] matchMechanisms = new String[1];
    matchMechanisms[0] = getMatchMechanism(new String(response.msg.data, Charset.defaultCharset()));
    if (matchMechanisms[0].equals("")) {
      throw new Exception("No matching mechanism was found");
    }

    status = negotiation_status.SASL_SELECT_MECHANISMS;
    blob msg = new blob(saslWrapper.init(matchMechanisms));
    send(status, msg);
  }

  void onMechanismSelected(negotiation_response response) throws Exception {
    checkStatus(response.status, negotiation_status.SASL_SELECT_MECHANISMS_RESP);

    status = negotiation_status.SASL_INITIATE;
    blob msg = saslWrapper.getInitialResponse();
    send(status, msg);
  }

  void onChallenge(negotiation_response response) throws Exception {
    switch (response.status) {
      case SASL_CHALLENGE:
        blob msg = saslWrapper.evaluateChallenge(response.msg.data);
        status = negotiation_status.SASL_CHALLENGE_RESP;
        send(status, msg);
        break;
      case SASL_SUCC:
        negotiationSucceed();
        break;
      default:
        throw new Exception("receive wrong negotiation msg type" + response.status.toString());
    }
  }

  public String getMatchMechanism(String respString) {
    String matchMechanism = "";
    String[] serverSupportMechanisms = respString.split(",");
    for (String serverSupportMechanism : serverSupportMechanisms) {
      if (expectedMechanisms.contains(serverSupportMechanism)) {
        matchMechanism = serverSupportMechanism;
        break;
      }
    }

    return matchMechanism;
  }

  public void checkStatus(negotiation_status status, negotiation_status expected_status)
      throws Exception {
    if (status != expected_status) {
      throw new Exception("status is " + status + " while expect " + expected_status);
    }
  }

  private void negotiationFailed() {
    status = negotiation_status.SASL_AUTH_FAIL;
    session.closeSession();
  }

  private void negotiationSucceed() {
    status = negotiation_status.SASL_SUCC;
    session.onAuthSucceed();
  }

  negotiation_status getStatus() {
    return status;
  }
}
