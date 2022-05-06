// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.multi_remove_request;
import com.xiaomi.infra.pegasus.apps.multi_remove_response;
import com.xiaomi.infra.pegasus.apps.rrdb;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;

/** Created by weijiesun on 16-12-8. */
public class rrdb_multi_remove_operator extends client_operator {
  public rrdb_multi_remove_operator(
      com.xiaomi.infra.pegasus.base.gpid gpid,
      String tableName,
      multi_remove_request request,
      long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  public String name() {
    return "multi_remove";
  }

  public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_MULTI_REMOVE", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    rrdb.multi_remove_args get_args = new rrdb.multi_remove_args(request);
    get_args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(org.apache.thrift.protocol.TProtocol iprot) throws TException {
    rrdb.multi_remove_result result = new rrdb.multi_remove_result();
    result.read(iprot);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "multi remove failed: unknown result");
  }

  public multi_remove_response get_response() {
    return resp;
  }

  private multi_remove_request request;
  private multi_remove_response resp;
}
