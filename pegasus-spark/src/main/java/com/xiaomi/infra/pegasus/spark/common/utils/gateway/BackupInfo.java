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

import java.io.Serializable;

public class BackupInfo implements Serializable {
  public static class Error {
    public String Errno;
  }

  public static class ExecuteRequest {
    public String TableName;
    public String BackupProvider;
    public String BackupPath;
  }

  public static class ExecuteResponse {
    public Error err;
    public String hint_message;
    public long backup_id;
  }

  public static class QueryResponse {
    public static class BackupItem {
      public long backup_id;
      public String app_name;
      public String backup_provider_type;
      public String backup_path;
      public long start_time_ms;
      public long end_time_ms;
      public boolean is_backup_failed;
    }

    public Error err;
    public String hint_message;
    public BackupItem[] backup_items;
  }
}
