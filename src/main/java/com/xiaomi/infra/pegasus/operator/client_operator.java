// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.base.gpid;
import com.xiaomi.infra.pegasus.tools.Tools;
import com.xiaomi.infra.pegasus.thrift.TException;
import com.xiaomi.infra.pegasus.rpc.ThriftHeader;

public abstract class client_operator {
    public client_operator(gpid gpid, String tableName) {
        this.header = new ThriftHeader();
        this.header.app_id = gpid.get_app_id();
        this.header.partition_index = gpid.get_pidx();
        this.pid = gpid;
        this.tableName = tableName;
        this.rpc_error = new error_code();
    }

    public final byte[] prepare_thrift_header(int body_length) {
        header.body_length = body_length;
        header.header_length = ThriftHeader.HEADER_LENGTH;
        header.thread_hash = Tools.dsn_gpid_to_thread_hash(header.app_id, header.partition_index);
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
        // pegasus.client.put.succ.qps
        return new StringBuilder()
                .append("pegasus.client.")
                .append(name())
                .append(".")
                .append(mark)
                .append(".qps@")
                .append(tableName)
                .toString();
    }

    public String getLatencyCounter() {
        // pegasus.client.put.latency
        return new StringBuilder()
                .append("pegasus.client.")
                .append(name())
                .append(".latency@")
                .append(tableName)
                .toString();
    }

    public final gpid get_gpid() { return pid; }
    public abstract String name();
    public abstract void send_data(com.xiaomi.infra.pegasus.thrift.protocol.TProtocol oprot, int sequence_id) throws TException;
    public abstract void recv_data(com.xiaomi.infra.pegasus.thrift.protocol.TProtocol iprot) throws TException;

    public ThriftHeader header;
    public gpid pid;
    public String tableName; // only for metrics
    public error_code rpc_error;
};
