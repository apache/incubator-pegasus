// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.apps.rrdb;
import com.xiaomi.infra.pegasus.apps.scan_request;
import com.xiaomi.infra.pegasus.apps.scan_response;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class rrdb_scan_operator extends client_operator {
  public rrdb_scan_operator(
      com.xiaomi.infra.pegasus.base.gpid gpid,
      String tableName,
      scan_request request,
      long partitionHash) {
    super(gpid, tableName, partitionHash);
    this.request = request;
  }

  public String name() {
    return "scan";
  }

  public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
    TMessage msg = new TMessage("RPC_RRDB_RRDB_SCAN", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    rrdb.scan_args args = new rrdb.scan_args(request);
    args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(TProtocol iprot) throws TException {
    rrdb.scan_result result = new rrdb.scan_result();
    result.read(iprot);
    if (result.isSetSuccess()) resp = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT, "scan failed: unknown result");
  }

  public scan_response get_response() {
    return resp;
  }

  private scan_request request;
  private scan_response resp;
}
