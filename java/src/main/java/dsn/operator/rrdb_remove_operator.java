// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.operator;

import dsn.apps.update_response;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.protocol.TProtocol;

import dsn.apps.rrdb;
import dsn.base.blob;

public class rrdb_remove_operator extends client_operator {
    public rrdb_remove_operator(dsn.base.gpid gpid, blob request) {
        super(gpid);
        this.request = request;
    }

    public String name() { return "remove"; }
    public void send_data(org.apache.thrift.protocol.TProtocol oprot, int seqid) throws TException {
        TMessage msg = new TMessage("RPC_RRDB_RRDB_REMOVE", TMessageType.CALL, seqid);
        oprot.writeMessageBegin(msg);
        rrdb.remove_args remove_args = new rrdb.remove_args(request);
        remove_args.write(oprot);
        oprot.writeMessageEnd();
    }

    public void recv_data(TProtocol iprot) throws TException {
        rrdb.put_result result = new rrdb.put_result();
        result.read(iprot);
        if (result.isSetSuccess())
            resp = result.success;
        else
            throw new org.apache.thrift.TApplicationException(org.apache.thrift.TApplicationException.MISSING_RESULT, "put failed: unknown result");
    }

    public update_response get_response() { return resp; }

    private blob request;
    private update_response resp;
}
