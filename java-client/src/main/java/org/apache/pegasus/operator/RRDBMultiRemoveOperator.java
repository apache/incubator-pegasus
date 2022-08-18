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

import org.apache.pegasus.apps.multi_remove_request;
import org.apache.pegasus.apps.multi_remove_response;
import org.apache.pegasus.apps.rrdb;
import org.apache.pegasus.base.gpid;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;

/** Created by weijiesun on 16-12-8. */
public class RRDBMultiRemoveOperator extends ClientOperator {

  private final multi_remove_request request;
  private multi_remove_response resp;

  public RRDBMultiRemoveOperator(
      gpid gpid, String tableName, multi_remove_request request, long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  @Override
  public String name() {
    return "multi_remove";
  }

  @Override
  public void sendData(org.apache.thrift.protocol.TProtocol out, int seqId) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_MULTI_REMOVE", TMessageType.CALL, seqId);
    out.writeMessageBegin(msg);
    rrdb.multi_remove_args get_args = new rrdb.multi_remove_args(request);
    get_args.write(out);
    out.writeMessageEnd();
  }

  @Override
  public void recvData(org.apache.thrift.protocol.TProtocol in) throws TException {
    rrdb.multi_remove_result result = new rrdb.multi_remove_result();
    result.read(in);
    if (result.isSetSuccess()) {
      resp = result.success;
    } else {
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "multi remove failed: unknown result");
    }
  }

  public multi_remove_response get_response() {
    return this.resp;
  }
}
