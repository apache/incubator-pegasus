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

/** Created by weijiesun on 16-11-8. */
import org.apache.pegasus.apps.meta;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.replication.query_cfg_request;
import org.apache.pegasus.replication.query_cfg_response;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class query_cfg_operator extends client_operator {
  public query_cfg_operator(gpid gpid, query_cfg_request request) {
    super(gpid, "", 0);
    this.request = request;
  }

  public String name() {
    return "query_config";
  }

  public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
    TMessage msg = new TMessage("RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    meta.query_cfg_args args = new meta.query_cfg_args(request);
    args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(TProtocol iprot) throws TException {
    meta.query_cfg_result result = new meta.query_cfg_result();
    result.read(iprot);
    if (result.isSetSuccess()) response = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "query config failed: unknown result");
  }

  public query_cfg_response get_response() {
    return response;
  }

  private query_cfg_request request;
  private query_cfg_response response;
}
