package com.xiaomi.infra.pegasus.spark.common;

import java.io.Serializable;

public class HDFSConfig implements Serializable {
  private String url;
  private String path;
  private String port;

  public HDFSConfig(String url, String Port) throws PegasusSparkException {
    String fsUrl;
    String fsPath;
    try {
      String[] urls = url.split("/");
      fsUrl = urls[0] + "//" + urls[2];

      String[] paths = url.split(fsUrl);
      fsPath = paths.length == 0 ? "/" : paths[1];
    } catch (RuntimeException e) {
      throw new PegasusSparkException("invalid hdfs url:" + url);
    }
    this.url = fsUrl;
    this.path = fsPath;
    this.port = Port;
  }

  public HDFSConfig(String url) throws PegasusSparkException {
    this(url, "0");
  }

  public void setUrl(String url) {
    this.url = url;
  }

  public String getPath() {
    return path;
  }

  public void setPort(String Port) {
    this.port = Port;
  }

  public String getUrl() {
    return url;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public String getPort() {
    return port;
  }
}
