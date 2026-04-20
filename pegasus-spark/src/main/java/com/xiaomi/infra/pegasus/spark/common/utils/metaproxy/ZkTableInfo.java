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

import java.util.Objects;

public class ZkTableInfo {
  public String cluster_name;
  public String meta_addrs;

  public ZkTableInfo(String cluster_name, String meta_addrs) {
    this.cluster_name = cluster_name;
    this.meta_addrs = meta_addrs;
  }

  @Override
  public String toString() {
    return "ZkTableInfo{"
        + "cluster_name='"
        + cluster_name
        + '\''
        + ", meta_addrs='"
        + meta_addrs
        + '\''
        + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ZkTableInfo tableInfo = (ZkTableInfo) o;
    return Objects.equals(cluster_name, tableInfo.cluster_name)
        && Objects.equals(meta_addrs, tableInfo.meta_addrs);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cluster_name, meta_addrs);
  }
}
