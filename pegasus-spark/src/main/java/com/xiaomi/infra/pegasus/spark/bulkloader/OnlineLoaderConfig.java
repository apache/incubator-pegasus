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

package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.xiaomi.infra.pegasus.client.ClientOptions;
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController.RateLimiterConfig;
import java.io.Serializable;
import java.util.List;

public class OnlineLoaderConfig implements Serializable {
  private ClientOptions clientOptions;
  private RateLimiterConfig rateLimiterConfig;

  private String clusterName;
  private String tableName;
  private int batchCount;

  public OnlineLoaderConfig(ClientOptions clientOptions, String clusterName, String tableName) {
    this.clientOptions = clientOptions;
    this.rateLimiterConfig = new RateLimiterConfig();
    this.clusterName = clusterName;
    this.tableName = tableName;
    this.batchCount = 10;
  }

  /**
   * set pegasus client options used for connecting pegasus online cluster, detail see {@link
   * ClientOptions}
   *
   * @param clientOptions
   * @return
   */
  public OnlineLoaderConfig setClientOptions(ClientOptions clientOptions) {
    this.clientOptions = clientOptions;
    return this;
  }

  /**
   * set RateLimiter config to control request flow that include `qpsLimiter` and `bytesLimiter`,
   * detail see {@link com.xiaomi.infra.pegasus.spark.common.utils.FlowController} and {@link
   * RateLimiterConfig}
   *
   * @param rateLimiterConfig see {@link RateLimiterConfig}
   * @return this
   */
  public OnlineLoaderConfig setRateLimiterConfig(RateLimiterConfig rateLimiterConfig) {
    this.rateLimiterConfig = rateLimiterConfig;
    return this;
  }

  /**
   * set cluster name
   *
   * @param clusterName
   */
  public void setClusterName(String clusterName) {
    this.clusterName = clusterName;
  }

  /**
   * set table name
   *
   * @param tableName
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * set batch count, {@link OnlineLoader} use {@link
   * com.xiaomi.infra.pegasus.client.PegasusClientInterface#batchSet(String, List)} to load data,
   * batchCount is used for `List` size
   *
   * @param batchCount
   * @return
   */
  public OnlineLoaderConfig setBatchCount(int batchCount) {
    this.batchCount = batchCount;
    return this;
  }

  public ClientOptions getClientOptions() {
    return clientOptions;
  }

  public RateLimiterConfig getRateLimiterConfig() {
    return rateLimiterConfig;
  }

  public String getClusterName() {
    return clusterName;
  }

  public String getTableName() {
    return tableName;
  }

  public int getBatchCount() {
    return batchCount;
  }
}
