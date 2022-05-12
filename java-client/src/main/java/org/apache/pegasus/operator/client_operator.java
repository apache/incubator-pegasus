/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pegasus.operator;

import org.apache.pegasus.base.error_code;
import org.apache.pegasus.base.gpid;
import org.apache.pegasus.replication.request_meta;
import org.apache.pegasus.rpc.ThriftHeader;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;

public abstract class client_operator {

  /**
   * Whether does this RPC support backup request.<br>
   * Generally, only read-operations support backup-request which sacrifices strong-consistency.
   */
  public boolean supportBackupRequest() {
    return false;
  }

  public client_operator(gpid gpid, String tableName, long partitionHash) {
    this.header = new ThriftHeader();
    this.meta = new request_meta();
    this.meta.setApp_id(gpid.get_app_id());
    this.meta.setPartition_index(gpid.get_pidx());
    this.meta.setPartition_hash(partitionHash);
    this.pid = gpid;
    this.tableName = tableName;
    this.rpc_error = new error_code();
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
}
