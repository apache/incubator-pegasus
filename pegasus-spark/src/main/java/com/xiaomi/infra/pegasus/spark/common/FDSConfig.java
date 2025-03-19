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

public class FDSConfig extends HDFSConfig {

  String bucketName;
  String endPoint;
  String accessKey;
  String accessSecret;

  public FDSConfig(
      String accessKey, String accessSecret, String bucketName, String endPoint, String port)
      throws PegasusSparkException {
    super("fds://" + accessKey + ":" + accessSecret + "@" + bucketName + "." + endPoint, port);
    this.accessKey = accessKey;
    this.accessSecret = accessSecret;
    this.bucketName = bucketName;
    this.endPoint = endPoint;
  }

  public FDSConfig(String accessKey, String accessSecret, String bucketName, String endPoint)
      throws PegasusSparkException {
    this(accessKey, accessSecret, bucketName, endPoint, "80");
  }

  public void setBucketName(String bucketName) {
    this.bucketName = bucketName;
  }

  public void setEndPoint(String endPoint) {
    this.endPoint = endPoint;
  }

  public void setAccessKey(String accessKey) {
    this.accessKey = accessKey;
  }

  public void setAccessSecret(String accessSecret) {
    this.accessSecret = accessSecret;
  }

  public String getBucketName() {
    return bucketName;
  }

  public String getEndPoint() {
    return endPoint;
  }

  public String getAccessKey() {
    return accessKey;
  }

  public String getAccessSecret() {
    return accessSecret;
  }
}
