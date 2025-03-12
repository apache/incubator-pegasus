package com.xiaomi.infra.pegasus.spark.common;

import com.xiaomi.infra.pegasus.spark.common.utils.FlowController.RateLimiterConfig;
import java.io.Serializable;

/** The config class contains the common config for coldBackupConfig and bulkLoadConfig */
public abstract class CommonConfig implements Serializable {

  private RemoteFileSystem remoteFileSystem;
  private RateLimiterConfig rateLimiterConfig;

  private String remoteFileSystemURL;
  private String remoteFileSystemPath;
  private String remoteFileSystemPort;
  private String clusterName;
  private String tableName;

  protected CommonConfig(HDFSConfig hdfsConfig, String clusterName, String tableName) {
    initConfig(hdfsConfig, clusterName, tableName);
    this.remoteFileSystem = new HDFSFileSystem();
  }

  protected CommonConfig(FDSConfig fdsConfig, String clusterName, String tableName) {
    initConfig(fdsConfig, clusterName, tableName);
    this.remoteFileSystem = new FDSFileSystem(fdsConfig);
  }

  private void initConfig(HDFSConfig config, String clusterName, String tableName) {
    this.rateLimiterConfig = new RateLimiterConfig();
    this.remoteFileSystemURL = config.getUrl();
    this.remoteFileSystemPath = config.getPath();
    this.remoteFileSystemPort = config.getPort();
    this.clusterName = clusterName;
    this.tableName = tableName;
  }

  public CommonConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    return this;
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

  public String getRemoteFileSystemPath() {
    return remoteFileSystemPath;
  }

  public void setRemoteFileSystemPath(String remoteFileSystemPath) {
    this.remoteFileSystemPath = remoteFileSystemPath;
  }

  public RemoteFileSystem getRemoteFileSystem() {
    return remoteFileSystem;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
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
