// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

/** Created by weijiesun on 16-11-8. */
import com.xiaomi.infra.pegasus.apps.meta;
import com.xiaomi.infra.pegasus.replication.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

public class query_cfg_operator extends client_operator {
  public query_cfg_operator(com.xiaomi.infra.pegasus.base.gpid gpid, query_cfg_request request) {
    super(gpid, "", 0);
    this.request = request;
  }

  public String name() {
    return "query_config";
  }

  public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
    TMessage msg = new TMessage("RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX", TMessageType.CALL, seqid);
    oprot.writeMessageBegin(msg);
    meta.query_cfg_args args = new meta.query_cfg_args(request);
    args.write(oprot);
    oprot.writeMessageEnd();
  }

  public void recv_data(TProtocol iprot) throws TException {
    meta.query_cfg_result result = new meta.query_cfg_result();
    result.read(iprot);
    if (result.isSetSuccess()) response = result.success;
    else
      throw new org.apache.thrift.TApplicationException(
          org.apache.thrift.TApplicationException.MISSING_RESULT,
          "query config failed: unknown result");
  }

  public query_cfg_response get_response() {
    return response;
  }

  private query_cfg_request request;
  private query_cfg_response response;
}
