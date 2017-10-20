// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.operator;

import dsn.apps.multi_get_request;
import dsn.apps.multi_get_response;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;

import dsn.apps.rrdb;

public class rrdb_multi_get_operator extends client_operator {
    public rrdb_multi_get_operator(dsn.base.gpid gpid, multi_get_request request) {
        super(gpid);
        this.request = request;
    }

    public String name() { return "multi_get"; }
    public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
        TMessage msg = new TMessage("RPC_RRDB_RRDB_MULTI_GET", TMessageType.CALL, seqid);
        oprot.writeMessageBegin(msg);
        rrdb.multi_get_args get_args = new rrdb.multi_get_args(request);
        get_args.write(oprot);
        oprot.writeMessageEnd();
    }

    public void recv_data(org.apache.thrift.protocol.TProtocol iprot) throws TException {
        rrdb.multi_get_result result = new rrdb.multi_get_result();
        result.read(iprot);
        if (result.isSetSuccess())
            resp = result.success;
        else
            throw new org.apache.thrift.TApplicationException(
                    org.apache.thrift.TApplicationException.MISSING_RESULT, "multi get failed: unknown result");
    }

    public multi_get_response get_response() { return resp; }
    private multi_get_request request;
    private multi_get_response resp;
}

