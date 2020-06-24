package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class Config implements Serializable {

  public String remoteFsUrl;
  public String remoteFsPort;

  public String clusterName;
  public String tableName;

  public RocksDBOptions rocksDBOptions;

  public Config(String remoteFsUrl, String remoteFsPort, String clusterName, String tableName)
      throws FDSException {
    this.remoteFsUrl = remoteFsUrl;
    this.remoteFsPort = remoteFsPort;
    this.clusterName = clusterName;
    this.tableName = tableName;

    this.rocksDBOptions = new RocksDBOptions(remoteFsUrl, remoteFsPort);
  }
}
