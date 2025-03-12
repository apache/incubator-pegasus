package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.common.utils.FlowController;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

class ColdBackupLoader implements PegasusLoader {
  private static final Log LOG = LogFactory.getLog(ColdBackupLoader.class);

  private final ColdBackupConfig coldBackupConfig;
  private final Map<Integer, String> checkpointUrls;
  private final int partitionCount;

  ColdBackupLoader(ColdBackupConfig config) throws PegasusSparkException {
    this.coldBackupConfig = config;
    this.checkpointUrls = PathEncoder.create(config).getCheckPointUrls();
    this.partitionCount = checkpointUrls.size();
    LOG.info("init fds default config and get the data urls");
  }

  @Override
  public int getPartitionCount() {
    return partitionCount;
  }

  @Override
  public ColdBackupConfig getConfig() {
    return coldBackupConfig;
  }

  @Override
  public PegasusScanner getScanner(int pid) throws PegasusSparkException, RocksDBException {
    RocksDBOptions rocksDBOptions =
        new RocksDBOptions(
            coldBackupConfig.getRemoteFileSystemURL(), coldBackupConfig.getRemoteFileSystemPort());
    rocksDBOptions.options.setMaxOpenFiles(coldBackupConfig.getFileOpenCount());
    rocksDBOptions.readOptions.setReadaheadSize(coldBackupConfig.getReadAheadSize());

    RocksDB rocksDB = RocksDB.openReadOnly(rocksDBOptions.options, checkpointUrls.get(pid));
    RocksIterator rocksIterator = rocksDB.newIterator(rocksDBOptions.readOptions);
    FlowController flowController =
        new FlowController(partitionCount, coldBackupConfig.getRateLimiterConfig());
    return new ColdBackupScanner(
        coldBackupConfig.getDataVersion(), rocksDBOptions, rocksDB, rocksIterator, flowController);
  }

  static class ColdBackupScanner implements PegasusScanner {
    DataVersion scannerVersion;
    RocksDBOptions rocksDBOptions;
    RocksDB rocksDB;
    RocksIterator rocksIterator;
    FlowController flowController;

    ColdBackupScanner(
        DataVersion scannerVersion,
        RocksDBOptions rocksDBOptions,
        RocksDB rocksDB,
        RocksIterator rocksIterator,
        FlowController flowController) {
      this.scannerVersion = scannerVersion;
      this.rocksDBOptions = rocksDBOptions;
      this.rocksDB = rocksDB;
      this.rocksIterator = rocksIterator;
      this.flowController = flowController;
    }

    @Override
    public boolean isValid() {
      return rocksIterator.isValid();
    }

    @Override
    public void seekToFirst() {
      rocksIterator.seekToFirst();
    }

    @Override
    public void next() {
      // `flowControl` initialized by `RateLimiterConfig` whose `qps` and `bytes` are both 0
      // default, which means if you don't set custom config value > 0 , it will not limit and
      // return immediately
      flowController.acquireQPS();
      flowController.acquireBytes(rocksIterator.key().length + rocksIterator.value().length);

      rocksIterator.next();
    }

    @Override
    public PegasusRecord restore() {
      if (scannerVersion != null) {
        return scannerVersion.restore(rocksIterator);
      }
      scannerVersion = new AutoDetectDataVersion();
      LOG.warn("user not set data version, will try auto detect data version");
      return scannerVersion.restore(rocksIterator);
    }

    @Override
    public void close() {
      rocksIterator.close();
      rocksDB.close();
      rocksDBOptions.close();
    }
  }
}
