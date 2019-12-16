package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class Config implements Serializable {

  private static final long UNIT = 1024 * 1024L;

  public String remoteFsUrl;
  public String remoteFsPort;

  public String clusterName;
  public String tableName;
  public int tableId;
  public int tablePartitionCount;

  public int dbMaxFileOpenCount = 50;
  public long dbReadAheadSize = 1024 * 1024L;

  public Config setRemote(String url, String port) {
    this.remoteFsUrl = url;
    this.remoteFsPort = port;
    return this;
  }

  public Config setTableInfo(String clusterName, String tableName) {
    this.clusterName = clusterName;
    this.tableName = tableName;
    return this;
  }

  public Config setTableId(int tableId) {
    this.tableId = tableId;
    return this;
  }

  public Config setTablePartitionCount(int tablePartitionCount) {
    this.tablePartitionCount = tablePartitionCount;
    return this;
  }

  public Config setDbMaxFileOpenCount(int dbMaxFileOpenCount) {
    this.dbMaxFileOpenCount = dbMaxFileOpenCount;
    return this;
  }

  public Config setDbReadAheadSize(long dbReadAheadSize) {
    this.dbReadAheadSize = dbReadAheadSize * UNIT;
    return this;
  }
}
