package com.xiaomi.infra.pegasus.spark.common.utils.gateway;

import java.io.Serializable;
import java.util.Map;

// http table info resp format
public class TableInfo implements Serializable {
  public class GeneralInfo {
    public String app_name;
    public String app_id;
    public String partition_count;
    public String max_replica_count;
  }

  public class PartitionInfo {
    public int pidx;
    public int ballot;
    public String replica_count;
    public String primary;
    public String secondaries;
  }

  public class NodesInfo {
    public String node;
    public int primary;
    public int secondary;
    public int total;
  }

  public class HealthInfo {
    public int fully_healthy_partition_count;
    public int unhealthy_partition_count;
    public int write_unhealthy_partition_count;
    public int read_unhealthy_partition_count;
  }

  public GeneralInfo general;
  public Map<Integer, PartitionInfo> replicas;
  public Map<String, NodesInfo> nodes;
  public HealthInfo healthy;
}
