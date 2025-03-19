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
