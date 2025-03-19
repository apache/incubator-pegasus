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
