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
