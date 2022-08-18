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

import org.apache.pegasus.apps.multi_put_request;
import org.apache.pegasus.apps.rrdb;
import org.apache.pegasus.apps.update_response;
import org.apache.pegasus.base.gpid;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class RRDBMultiPutOperator extends ClientOperator {

  private final multi_put_request request;
  private update_response resp;

  public RRDBMultiPutOperator(
      gpid gpid, String tableName, multi_put_request request, long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  @Override
  public String name() {
    return "multi_put";
  }

  @Override
  public void sendData(org.apache.thrift.protocol.TProtocol out, int seqId) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_MULTI_PUT", TMessageType.CALL, seqId);
    out.writeMessageBegin(msg);
    rrdb.multi_put_args put_args = new rrdb.multi_put_args(request);
    put_args.write(out);
    out.writeMessageEnd();
  }

  @Override
  public void recvData(TProtocol in) throws TException {
    rrdb.multi_put_result result = new rrdb.multi_put_result();
    result.read(in);
    if (result.isSetSuccess()) {
      resp = result.success;
    } else {
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "multi put failed: unknown result");
    }
  }

  public update_response get_response() {
    return resp;
  }

  public multi_put_request get_request() {
    return request;
  }
}
