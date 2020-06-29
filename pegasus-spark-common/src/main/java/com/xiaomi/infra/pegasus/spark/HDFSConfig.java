package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class HDFSConfig implements Serializable {
  String remoteFsUrl;
  String remoteFsPort;

  public HDFSConfig(String remoteFsUrl, String remoteFsPort) {
    this.remoteFsUrl = remoteFsUrl;
    this.remoteFsPort = remoteFsPort;
  }

  public void setRemoteFsUrl(String remoteFsUrl) {
    this.remoteFsUrl = remoteFsUrl;
  }

  public void setRemoteFsPort(String remoteFsPort) {
    this.remoteFsPort = remoteFsPort;
  }

  public String getRemoteFsUrl() {
    return remoteFsUrl;
  }

  public String getRemoteFsPort() {
    return remoteFsPort;
  }
}
