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
