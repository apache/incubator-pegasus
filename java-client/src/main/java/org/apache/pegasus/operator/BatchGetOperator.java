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

import org.apache.pegasus.apps.batch_get_request;
import org.apache.pegasus.apps.batch_get_response;
import org.apache.pegasus.apps.rrdb;
import org.apache.pegasus.base.gpid;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class BatchGetOperator extends ClientOperator {
  private final batch_get_request request;
  private batch_get_response response;

  public BatchGetOperator(
      gpid gpid, String tableName, batch_get_request request, long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  @Override
  public String name() {
    return "batch_get";
  }

  @Override
  public void sendData(TProtocol out, int sequenceId) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_BATCH_GET", TMessageType.CALL, sequenceId);
    out.writeMessageBegin(msg);
    rrdb.batch_get_args get_args = new rrdb.batch_get_args(request);
    get_args.write(out);
    out.writeMessageEnd();
  }

  @Override
  public void recvData(TProtocol in) throws TException {
    rrdb.batch_get_result result = new rrdb.batch_get_result();
    result.read(in);
    if (result.isSetSuccess()) {
      response = result.success;
    } else {
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "Batch Get failed: unknown result");
    }
  }

  public batch_get_response get_response() {
    return response;
  }
}
