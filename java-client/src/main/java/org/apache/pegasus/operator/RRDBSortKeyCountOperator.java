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

import org.apache.pegasus.apps.count_response;
import org.apache.pegasus.apps.rrdb;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.base.gpid;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;

/** Created by weijiesun on 16-12-8. */
public class RRDBSortKeyCountOperator extends ClientOperator {

  private final blob request;
  private count_response resp;

  public RRDBSortKeyCountOperator(gpid gpid, String tableName, blob request, long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  @Override
  public String name() {
    return "sortkey_count";
  }

  @Override
  public void sendData(org.apache.thrift.protocol.TProtocol out, int seqId) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_SORTKEY_COUNT", TMessageType.CALL, seqId);
    out.writeMessageBegin(msg);
    rrdb.sortkey_count_args get_args = new rrdb.sortkey_count_args(request);
    get_args.write(out);
    out.writeMessageEnd();
  }

  @Override
  public void recvData(org.apache.thrift.protocol.TProtocol in) throws TException {
    rrdb.sortkey_count_result result = new rrdb.sortkey_count_result();
    result.read(in);
    if (result.isSetSuccess()) {
      resp = result.success;
    } else {
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "get sortkey count failed: unknown result");
    }
  }

  public count_response get_response() {
    return resp;
  }
}
