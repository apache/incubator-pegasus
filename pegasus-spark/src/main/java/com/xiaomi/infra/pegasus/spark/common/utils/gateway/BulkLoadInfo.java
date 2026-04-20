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

package com.xiaomi.infra.pegasus.spark.common.utils.gateway;

public class BulkLoadInfo {

  public static class Error {
    public String Errno;

    public Error(String errno) {
      Errno = errno;
    }
  }

  public static class ExecuteRequest {
    public String ClusterName;
    public String TableName;
    public String RemoteProvider;
    public String RemotePath;
  }

  public static class ExecuteResponse {
    public BulkLoadInfo.Error err;
    public String hint_msg;
  }

  public static class QueryResponse {
    public BulkLoadInfo.Error err;
    public String app_name;
    public String app_status;
    public String[] partitions_status;
    public int max_replica_count;
    public String bulk_load_states;
    public String hint_msg;
  }

  public static class CancelRequest {
    public String ClusterName;
    public String TableName;
  }

  public static class CancelResponse {
    public BulkLoadInfo.Error err;
    public String hint_msg;
  }
}
