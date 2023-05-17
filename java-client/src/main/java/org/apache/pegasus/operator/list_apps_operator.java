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

import org.apache.pegasus.base.gpid;
import org.apache.pegasus.replication.admin_client.list_apps_args;
import org.apache.pegasus.replication.admin_client.list_apps_result;
import org.apache.pegasus.replication.configuration_list_apps_request;
import org.apache.pegasus.replication.configuration_list_apps_response;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class list_apps_operator extends client_operator {
  public list_apps_operator(configuration_list_apps_request request) {
    super(new gpid(), "", 0);
    this.request = request;
  }

  @Override
  public String name() {
    return "list_apps_operator";
  }

  @Override
  public void send_data(TProtocol oprot, int sequence_id) throws TException {
    TMessage msg = new TMessage("RPC_CM_LIST_APPS", TMessageType.CALL, sequence_id);
    oprot.writeMessageBegin(msg);
    list_apps_args args = new list_apps_args(request);
    args.write(oprot);
    oprot.writeMessageEnd();
  }

  @Override
  public void recv_data(TProtocol iprot) throws TException {
    list_apps_result result = new list_apps_result();
    result.read(iprot);
    if (result.isSetSuccess()) response = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "list apps failed: unknown result");
  }

  public configuration_list_apps_response get_response() {
    return response;
  }

  private configuration_list_apps_request request;
  private configuration_list_apps_response response;
}
