package com.xiaomi.infra.pegasus.spark.analyser;

import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import java.io.Serializable;
import org.rocksdb.RocksDBException;

public interface PegasusLoader extends Serializable {

  int getPartitionCount();

  ColdBackupConfig getConfig();

  PegasusScanner getScanner(int pid) throws PegasusSparkException, RocksDBException;
}
