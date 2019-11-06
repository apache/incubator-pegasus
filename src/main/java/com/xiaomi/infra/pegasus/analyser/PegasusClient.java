package com.xiaomi.infra.pegasus.analyser;

import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.rocksdb.*;

public class PegasusClient implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private static final Log LOG = LogFactory.getLog(PegasusClient.class);

  private RocksDBOptions rocksDBOptions;
  private int partitionCount;
  private Map<Integer, String> checkPointUrls;
  private PegasusScanner pegasusScanner;

  public PegasusClient(Config cfg, FDSService fdsService) {
    this.partitionCount = fdsService.getPartitionCount();
    this.checkPointUrls = fdsService.getCheckpointUrls();
    this.rocksDBOptions = new RocksDBOptions(cfg);
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public PegasusScanner getScanner(int pid) throws RocksDBException {
    LOG.info("open read only and get the " + pid + " scanner");
    RocksDB rocksDB = RocksDB.openReadOnly(rocksDBOptions.options, checkPointUrls.get(pid));
    RocksIterator rocksIterator = rocksDB.newIterator(rocksDBOptions.readOptions);
    pegasusScanner = new PegasusScanner(rocksDB, rocksIterator);
    return pegasusScanner;
  }

  public int getDataCount(int pid) throws RocksDBException {
    int count = 0;
    LOG.info("start count the " + pid + " data:" + count);
    PegasusScanner pegasusScanner = getScanner(pid);
    for (pegasusScanner.seekToFirst(); pegasusScanner.isValid(); pegasusScanner.next()) {
      count++;
      if (count % 1000000 == 0) {
        LOG.info("now the data count:" + count);
      }
    }
    pegasusScanner.close();
    return count;
  }

  @Override
  public void close() {
    pegasusScanner.close();
    rocksDBOptions.close();
  }
}
