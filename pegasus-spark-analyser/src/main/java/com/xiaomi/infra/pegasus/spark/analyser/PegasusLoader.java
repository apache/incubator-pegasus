package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.Config;
import java.io.Serializable;
import java.util.Map;
import org.rocksdb.RocksIterator;

public interface PegasusLoader extends Serializable {

  int getPartitionCount();

  Map<Integer, String> getCheckpointUrls();

  Config getConfig();

  PegasusRecord restoreRecord(RocksIterator rocksIterator);
}
