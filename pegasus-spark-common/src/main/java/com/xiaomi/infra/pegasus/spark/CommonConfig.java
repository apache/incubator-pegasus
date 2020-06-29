package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

/** The config class contains the common config for coldBackupConfig and bulkLoadConfig */
public class CommonConfig implements Serializable {

  private RemoteFileSystem remoteFileSystem;

  private String remoteFileSystemURL;
  private String remoteFileSystemPort;
  private String clusterName;
  private String tableName;

  public CommonConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    initConfig(hdfsConfig, clusterName, tableName);
    this.remoteFileSystem = new HDFSFileSystem();
  }

  public CommonConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    initConfig(fdsConfig, clusterName, tableName);
    this.remoteFileSystem = new FDSFileSystem(fdsConfig);
  }

  private void initConfig(HDFSConfig config, String clusterName, String tableName) {
    this.remoteFileSystemURL = config.remoteFsUrl;
    this.remoteFileSystemPort = config.remoteFsPort;
    this.clusterName = clusterName;
    this.tableName = tableName;
  }

  public void setRemoteFileSystemURL(String remoteFileSystemURL) {
    this.remoteFileSystemURL = remoteFileSystemURL;
  }

  public void setRemoteFileSystemPort(String remoteFileSystemPort) {
    this.remoteFileSystemPort = remoteFileSystemPort;
  }

  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public RemoteFileSystem getRemoteFileSystem() {
    return remoteFileSystem;
  }

  public String getRemoteFileSystemURL() {
    return remoteFileSystemURL;
  }

  public String getRemoteFileSystemPort() {
    return remoteFileSystemPort;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }
}
