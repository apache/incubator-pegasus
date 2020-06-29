package com.xiaomi.infra.pegasus.spark.analyser;

import java.io.Serializable;
import java.util.Map;
import org.rocksdb.RocksIterator;

public interface PegasusLoader extends Serializable {

  int getPartitionCount();

  Map<Integer, String> getCheckpointUrls();

  ColdBackupConfig getConfig();

  PegasusRecord restoreRecord(RocksIterator rocksIterator);
}
