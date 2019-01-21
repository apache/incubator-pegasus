// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.rrdb;
import com.xiaomi.infra.pegasus.apps.update_response;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.thrift.TException;
import com.xiaomi.infra.pegasus.thrift.protocol.TMessage;
import com.xiaomi.infra.pegasus.thrift.protocol.TMessageType;
import com.xiaomi.infra.pegasus.thrift.protocol.TProtocol;

public class rrdb_remove_operator extends client_operator {
  public rrdb_remove_operator(
      com.xiaomi.infra.pegasus.base.gpid gpid, String tableName, blob request) {
    super(gpid, tableName);
    this.request = request;
  }

  public String name() {
    return "remove";
  }

  public void send_data(com.xiaomi.infra.pegasus.thrift.protocol.TProtocol oprot, int seqid)
      throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_REMOVE", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    rrdb.remove_args remove_args = new rrdb.remove_args(request);
    remove_args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(TProtocol iprot) throws TException {
    rrdb.put_result result = new rrdb.put_result();
    result.read(iprot);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new com.xiaomi.infra.pegasus.thrift.TApplicationException(
          com.xiaomi.infra.pegasus.thrift.TApplicationException.MISSING_RESULT,
          "remove failed: unknown result");
  }

  public update_response get_response() {
    return resp;
  }

  private blob request;
  private update_response resp;
}
