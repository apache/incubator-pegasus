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

  private PegasusOptions pegasusOptions;
  private int partitionCount;
  private Map<Integer, String> checkPointUrls;
  private PegasusScanner pegasusScanner;

  public PegasusClient(PegasusOptions options, FdsService fdsService) {
    this.partitionCount = fdsService.getPartitionCount();
    this.checkPointUrls = fdsService.getCheckpointUrls();
    this.pegasusOptions = options;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public PegasusScanner getScanner(int pid) throws RocksDBException {
    pegasusScanner = getScanner(pegasusOptions, pid);
    return pegasusScanner;
  }

  public int getDataCount(int pid) throws RocksDBException {
    int count = 0;
    LOG.info("start count data:" + count);
    PegasusScanner pegasusScanner = getScanner(pegasusOptions, pid);
    for (pegasusScanner.seekToFirst(); pegasusScanner.isValid(); pegasusScanner.next()) {
      count++;
      if (count % 1000000 == 0) {
        LOG.info("now the  data counter:" + count);
      }
    }
    pegasusScanner.close();
    return count;
  }

  private PegasusScanner getScanner(PegasusOptions pegasusOptions, int pid)
      throws RocksDBException {
    LOG.info("open read only the " + pid + " partition count");
    RocksDB rocksDB = RocksDB.openReadOnly(pegasusOptions.options, checkPointUrls.get(pid));
    RocksIterator rocksIterator = rocksDB.newIterator(pegasusOptions.readOptions);
    return new PegasusScanner(rocksDB, rocksIterator);
  }

  @Override
  public void close() {
    pegasusScanner.close();
    pegasusOptions.close();
  }
}
