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

import com.xiaomi.infra.pegasus.client.HashKeyData;
import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusClientFactory;
import com.xiaomi.infra.pegasus.client.PegasusClientInterface;
import com.xiaomi.infra.pegasus.client.SetItem;
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OnlineLoader {
  private static final Log LOG = LogFactory.getLog(OnlineLoader.class);

  private PegasusClientInterface client;
  private FlowController flowController;
  private OnlineLoaderConfig onlineLoaderConfig;

  public OnlineLoader(OnlineLoaderConfig config, int partitionTaskCount) throws PException {
    this.onlineLoaderConfig = config;
    this.client = PegasusClientFactory.createClient(config.getClientOptions());
    this.flowController = new FlowController(partitionTaskCount, config.getRateLimiterConfig());
  }

  public void loadSingleItem(List<SetItem> setItems) throws InterruptedException {
    boolean success = false;
    while (!success) {
      try {
        int bytes = 0;
        for (SetItem setItem : setItems) {
          bytes += setItem.hashKey.length + setItem.sortKey.length + setItem.value.length;
        }

        // `flowControl` initialized by `RateLimiterConfig` whose `qps` and `bytes` are both 0
        // default, which means if you don't set custom config value > 0 , it will not limit and
        // return immediately
        flowController.acquireQPS(setItems.size());
        flowController.acquireBytes(bytes);

        client.batchSet(onlineLoaderConfig.getTableName(), setItems);
        success = true;
      } catch (PException e) {
        LOG.info("batchSet error:" + e);
        Thread.sleep(100);
      }
    }
  }

  public void loadMultiItem(List<HashKeyData> multiItems, int ttl) throws InterruptedException {
    boolean success = false;
    while (!success) {
      try {
        int bytes = 0;
        for (HashKeyData multiItem : multiItems) {
          bytes = multiItem.hashKey.length;
          for (Pair<byte[], byte[]> value : multiItem.values) {
            bytes += value.getKey().length + value.getValue().length;
          }
        }

        // `flowControl` initialized by `RateLimiterConfig` whose `qps` and `bytes` are both 0
        // default, which means if you don't set custom config value > 0 , it will not limit and
        // return immediately
        flowController.acquireQPS(multiItems.size());
        flowController.acquireBytes(bytes);

        client.batchMultiSet(onlineLoaderConfig.getTableName(), multiItems, ttl);
        success = true;
      } catch (PException e) {
        LOG.info("batchSet error:" + e);
        Thread.sleep(100);
      }
    }
  }

  public void close() {
    client.close();
  }
}
