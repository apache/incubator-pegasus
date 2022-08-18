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

import org.apache.pegasus.apps.meta;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.replication.configuration_create_app_request;
import org.apache.pegasus.replication.configuration_create_app_response;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class CreateAppOperator extends ClientOperator {

  private final configuration_create_app_request request;
  private configuration_create_app_response response;

  public CreateAppOperator(String appName, configuration_create_app_request request) {
    super(new gpid(), appName, 0);
    this.request = request;
  }

  @Override
  public String name() {
    return "create_app_operator";
  }

  @Override
  public void sendData(TProtocol out, int sequence_id) throws TException {
    TMessage msg = new TMessage("RPC_CM_CREATE_APP", TMessageType.CALL, sequence_id);
    out.writeMessageBegin(msg);
    org.apache.pegasus.apps.meta.create_app_args args = new meta.create_app_args(request);
    args.write(out);
    out.writeMessageEnd();
  }

  @Override
  public void recvData(TProtocol in) throws TException {
    meta.create_app_result result = new meta.create_app_result();
    result.read(in);
    if (result.isSetSuccess()) response = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "create app failed: unknown result");
  }

  public configuration_create_app_response get_response() {
    return response;
  }
}
