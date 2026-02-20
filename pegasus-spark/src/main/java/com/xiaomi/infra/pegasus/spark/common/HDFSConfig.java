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
