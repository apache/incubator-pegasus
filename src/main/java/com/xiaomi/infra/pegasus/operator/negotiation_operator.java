// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.negotiation_request;
import com.xiaomi.infra.pegasus.apps.negotiation_response;
import com.xiaomi.infra.pegasus.apps.security;
import com.xiaomi.infra.pegasus.base.gpid;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class negotiation_operator extends client_operator {
  public negotiation_operator(negotiation_request request) {
    super(new gpid(), "", 0);
    this.request = request;
  }

  public String name() {
    return "negotiate";
  }

  public void send_data(TProtocol oprot, int seqid) throws TException {
    TMessage msg = new TMessage("RPC_NEGOTIATION", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    security.negotiate_args get_args = new security.negotiate_args(request);
    get_args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(TProtocol iprot) throws TException {
    security.negotiate_result result = new security.negotiate_result();
    result.read(iprot);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new TApplicationException(
          TApplicationException.MISSING_RESULT, "get failed: unknown result");
  }

  public negotiation_response get_response() {
    return resp;
  }

  private negotiation_request request;
  private negotiation_response resp;
}
