// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

package dsn.operator;

import dsn.base.error_code;
import dsn.utils.tools;
import org.apache.thrift.TException;
import dsn.rpc.ThriftHeader;

public abstract class client_operator {
    public client_operator(dsn.base.gpid gpid) {
        this.header = new ThriftHeader();
        this.header.app_id = gpid.get_app_id();
        this.header.partition_index = gpid.get_pidx();
        this.pid = gpid;
        this.rpc_error = new dsn.base.error_code();
    }
    public final byte[] prepare_thrift_header(int body_length)
    {
        header.body_length = body_length;
        header.header_length = ThriftHeader.HEADER_LENGTH;
        header.thread_hash = tools.dsn_gpid_to_thread_hash(header.app_id, header.partition_index);
        header.partition_hash = 0;
        return header.toByteArray();
    }

    public String getQPSCounter() {
        String mark;
        switch (rpc_error.errno) {
            case ERR_OK:
                mark = "succ";
                break;
            case ERR_TIMEOUT:
                mark = "timeout";
                break;
            default:
                mark = "fail";
                break;
        }
        // pegasus.client.put.succ.qps#1
        return new StringBuilder()
                .append("pegasus.client.")
                .append(name())
                .append(".")
                .append(mark)
                .append(".qps#")
                .append(pid.get_app_id())
                .toString();
    }

    public String getLatencyCounter() {
        // pegasus.client.put.latency#1
        return new StringBuilder()
                .append("pegasus.client.")
                .append(name())
                .append(".latency#")
                .append(pid.get_app_id())
                .toString();
    }

    public final dsn.base.gpid get_gpid() { return pid; }
    public abstract String name();
    public abstract void send_data(org.apache.thrift.protocol.TProtocol oprot, int sequence_id) throws TException;
    public abstract void recv_data(org.apache.thrift.protocol.TProtocol iprot) throws TException;

    public ThriftHeader header;
    public dsn.base.gpid pid;
    public dsn.base.error_code rpc_error;
};
