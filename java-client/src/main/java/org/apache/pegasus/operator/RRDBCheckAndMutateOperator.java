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

import org.apache.pegasus.apps.check_and_mutate_request;
import org.apache.pegasus.apps.check_and_mutate_response;
import org.apache.pegasus.apps.rrdb;
import org.apache.pegasus.base.gpid;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class RRDBCheckAndMutateOperator extends ClientOperator {

  private final check_and_mutate_request request;
  private check_and_mutate_response resp;

  public RRDBCheckAndMutateOperator(
      gpid gpid, String tableName, check_and_mutate_request request, long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  public String name() {
    return "check_and_mutate";
  }

  @Override
  public void sendData(org.apache.thrift.protocol.TProtocol out, int seqId) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_CHECK_AND_MUTATE", TMessageType.CALL, seqId);
    out.writeMessageBegin(msg);
    rrdb.check_and_mutate_args incr_args = new rrdb.check_and_mutate_args(request);
    incr_args.write(out);
    out.writeMessageEnd();
  }

  @Override
  public void recvData(TProtocol in) throws TException {
    rrdb.check_and_mutate_result result = new rrdb.check_and_mutate_result();
    result.read(in);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "check_and_mutate failed: unknown result");
  }

  public check_and_mutate_response get_response() {
    return resp;
  }

  public check_and_mutate_request get_request() {
    return request;
  }
}
