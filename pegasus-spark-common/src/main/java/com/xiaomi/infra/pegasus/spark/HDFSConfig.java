package com.xiaomi.infra.pegasus.spark;

import java.io.Serializable;

public class HDFSConfig implements Serializable {
  private String url;
  private String Port;

  public HDFSConfig(String url, String Port) {
    this.url = url;
    this.Port = Port;
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public void setPort(String Port) {
    this.Port = Port;
  }

  public String getUrl() {
    return url;
  }

  public String getPort() {
    return Port;
  }
}
