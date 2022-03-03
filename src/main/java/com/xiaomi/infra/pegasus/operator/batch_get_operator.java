// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.batch_get_request;
import com.xiaomi.infra.pegasus.apps.batch_get_response;
import com.xiaomi.infra.pegasus.apps.rrdb;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class batch_get_operator extends client_operator {
  private batch_get_request request;
  private batch_get_response response;

  public batch_get_operator(
      com.xiaomi.infra.pegasus.base.gpid gpid,
      String tableName,
      batch_get_request request,
      long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  @Override
  public String name() {
    return "batch_get";
  }

  @Override
  public void send_data(TProtocol oprot, int sequence_id) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_BATCH_GET", TMessageType.CALL, sequence_id);
    oprot.writeMessageBegin(msg);
    rrdb.batch_get_args get_args = new rrdb.batch_get_args(request);
    get_args.write(oprot);
    oprot.writeMessageEnd();
  }

  @Override
  public void recv_data(TProtocol iprot) throws TException {
    rrdb.batch_get_result result = new rrdb.batch_get_result();
    result.read(iprot);
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
