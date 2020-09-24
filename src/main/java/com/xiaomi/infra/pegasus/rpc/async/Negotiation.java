package com.xiaomi.infra.pegasus.rpc.async;

import com.xiaomi.infra.pegasus.apps.negotiation_response;
import com.xiaomi.infra.pegasus.apps.negotiation_status;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.operator.negotiation_operator;
import com.xiaomi.infra.pegasus.rpc.ReplicationException;
import java.util.HashMap;
import javax.security.auth.Subject;
import javax.security.sasl.Sasl;
import org.slf4j.Logger;

public class Negotiation {
  private static final Logger logger = org.slf4j.LoggerFactory.getLogger(Negotiation.class);
  private negotiation_status status;
  private ReplicaSession session;
  private String serviceName; // used for SASL authentication
  private String serviceFqdn; // name used for SASL authentication
  private final HashMap<String, Object> props = new HashMap<String, Object>();
  private final Subject subject;

  public Negotiation(
      ReplicaSession session, Subject subject, String serviceName, String serviceFqdn) {
    this.session = session;
    this.subject = subject;
    this.serviceName = serviceName;
    this.serviceFqdn = serviceFqdn;
    this.props.put(Sasl.QOP, "auth");
  }

  public void start() {
    status = negotiation_status.SASL_LIST_MECHANISMS;
    send(status, new blob(new byte[0]));
  }

  public void send(negotiation_status status, blob msg) {
    // TODO: send negotiation message, using RecvHandler to handle the corresponding response.
  }

  private class RecvHandler implements Runnable {
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

  public negotiation_status get_status() {
    return status;
  }
}
