// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.incr_request;
import com.xiaomi.infra.pegasus.apps.incr_response;
import com.xiaomi.infra.pegasus.apps.rrdb;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class rrdb_incr_operator extends client_operator {
  public rrdb_incr_operator(
      com.xiaomi.infra.pegasus.base.gpid gpid,
      String tableName,
      incr_request request,
      long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  public String name() {
    return "incr";
  }

  public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_INCR", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    rrdb.incr_args incr_args = new rrdb.incr_args(request);
    incr_args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(TProtocol iprot) throws TException {
    rrdb.incr_result result = new rrdb.incr_result();
    result.read(iprot);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT, "incr failed: unknown result");
  }

  public incr_response get_response() {
    return resp;
  }

  private incr_request request;
  private incr_response resp;
}
