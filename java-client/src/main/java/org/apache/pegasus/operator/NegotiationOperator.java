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
package org.apache.pegasus.operator;

import org.apache.pegasus.apps.negotiation_request;
import org.apache.pegasus.apps.negotiation_response;
import org.apache.pegasus.apps.security;
import org.apache.pegasus.base.gpid;
import org.apache.thrift.TApplicationException;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class NegotiationOperator extends ClientOperator {

  private final negotiation_request request;
  private negotiation_response resp;

  public NegotiationOperator(negotiation_request request) {
    super(new gpid(), "", 0);
    this.request = request;
  }

  public String name() {
    return "negotiate";
  }

  public void sendData(TProtocol out, int seqId) throws TException {
    TMessage msg = new TMessage("RPC_NEGOTIATION", TMessageType.CALL, seqId);
    out.writeMessageBegin(msg);
    security.negotiate_args get_args = new security.negotiate_args(request);
    get_args.write(out);
    out.writeMessageEnd();
  }

  public void recvData(TProtocol in) throws TException {
    security.negotiate_result result = new security.negotiate_result();
    result.read(in);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new TApplicationException(
          TApplicationException.MISSING_RESULT, "get failed: unknown result");
  }

  public negotiation_response get_response() {
    return resp;
  }
}
