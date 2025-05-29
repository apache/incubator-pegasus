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

package com.xiaomi.infra.pegasus.spark.common.utils.metaproxy;

public class ClusterStateInfo {
  public String balance_operation_count;
  public String cluster_name;
  public String meta_function_level;
  public String meta_servers;
  public String primary_meta_server;
  public String primary_replica_count_stddev;
  public String total_replica_count_stddev;
  public String zookeeper_hosts;
  public String zookeeper_root;
}
