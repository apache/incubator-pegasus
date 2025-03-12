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
