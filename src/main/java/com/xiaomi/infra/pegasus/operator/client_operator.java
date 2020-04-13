// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.operator;

import com.xiaomi.infra.pegasus.base.error_code;
import com.xiaomi.infra.pegasus.base.gpid;
import com.xiaomi.infra.pegasus.replication.request_meta;
import com.xiaomi.infra.pegasus.rpc.ThriftHeader;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

public abstract class client_operator {
  public client_operator(gpid gpid, String tableName, boolean enableBackupRequest) {
    this.header = new ThriftHeader();
    this.meta = new request_meta();
    this.meta.setApp_id(gpid.get_app_id());
    this.meta.setPartition_index(gpid.get_pidx());
    this.pid = gpid;
    this.tableName = tableName;
    this.rpc_error = new error_code();
    this.enableBackupRequest = enableBackupRequest;
  }

  public client_operator(
      gpid gpid, String tableName, long partitionHash, boolean enableBackupRequest) {
    this(gpid, tableName, enableBackupRequest);
    this.meta.setPartition_hash(partitionHash);
  }

  public client_operator(gpid gpid, String tableName, long partitionHash) {
    this(gpid, tableName, false);
    this.meta.setPartition_hash(partitionHash);
  }

  public final byte[] prepare_thrift_header(int meta_length, int body_length) {
    this.header.meta_length = meta_length;
    this.header.body_length = body_length;
    return header.toByteArray();
  }

  public final void prepare_thrift_meta(
      TProtocol oprot, int client_timeout, boolean isBackupRequest) throws TException {
    this.meta.setClient_timeout(client_timeout);
    this.meta.setIs_backup_request(isBackupRequest);
    this.meta.write(oprot);
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

  public final gpid get_gpid() {
    return pid;
  }

  public abstract String name();

  public abstract void send_data(org.apache.thrift.protocol.TProtocol oprot, int sequence_id)
      throws TException;

  public abstract void recv_data(org.apache.thrift.protocol.TProtocol iprot) throws TException;

  public ThriftHeader header;
  public request_meta meta;
  public gpid pid;
  public String tableName; // only for metrics
  public error_code rpc_error;
  public boolean enableBackupRequest;
}
