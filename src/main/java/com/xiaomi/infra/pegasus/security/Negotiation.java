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
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import org.slf4j.Logger;

class Negotiation {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Negotiation.class);
  private negotiation_status status;
  private ReplicaSession session;
  private String serviceName; // used for SASL authentication
  private String serviceFqdn; // name used for SASL authentication
  private final HashMap<String, Object> props = new HashMap<String, Object>();
  private final Subject subject;

  // Because negotiation message is always the first rpc sent to pegasus server,
  // which will cost much more time. so we set negotiation timeout to 10s here
  private static final int negotiationTimeoutMS = 10000;

  Negotiation(ReplicaSession session, Subject subject, String serviceName, String serviceFqdn) {
    this.session = session;
    this.subject = subject;
    this.serviceName = serviceName;
    this.serviceFqdn = serviceFqdn;
    this.props.put(Sasl.QOP, "auth");
  }

  void start() {
    status = negotiation_status.SASL_LIST_MECHANISMS;
    send(status, new blob(new byte[0]));
  }

  void send(negotiation_status status, blob msg) {
    negotiation_request request = new negotiation_request(status, msg);
    negotiation_operator operator = new negotiation_operator(request);
    session.asyncSend(operator, new RecvHandler(operator), negotiationTimeoutMS, false);
  }

  private static class RecvHandler implements Runnable {
    negotiation_operator op;

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
      }
    }

    private void handleResponse() throws Exception {
      final negotiation_response resp = op.get_response();
      if (resp == null) {
        throw new Exception("RecvHandler received a null response, abandon it");
      }

      switch (resp.status) {
        case SASL_LIST_MECHANISMS_RESP:
        case SASL_SELECT_MECHANISMS_RESP:
        case SASL_CHALLENGE:
        case SASL_SUCC:
          break;
        default:
          throw new Exception("Received an unexpected response, status " + resp.status);
      }
    }
  }

  negotiation_status get_status() {
    return status;
  }
}
