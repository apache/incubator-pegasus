package com.xiaomi.infra.pegasus.spark.bulkloader;

import com.github.rholder.retry.RetryException;
import com.github.rholder.retry.Retryer;
import com.xiaomi.infra.pegasus.spark.common.PegasusSparkException;
import com.xiaomi.infra.pegasus.spark.common.RocksDBOptions;
import com.xiaomi.infra.pegasus.spark.common.utils.AutoRetryer;
import java.util.concurrent.ExecutionException;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

/** The wrapper of sstFileWriter of rocksdbjava */
class SstFileWriterWrapper {

  private SstFileWriter sstFileWriter;
  private Retryer<Boolean> booleanRetryer = AutoRetryer.getDefaultRetryer();
  private Retryer<Integer> integerRetryer = AutoRetryer.getDefaultRetryer();

  SstFileWriterWrapper(RocksDBOptions rocksDBOptions) {
    this.sstFileWriter = new SstFileWriter(rocksDBOptions.envOptions, rocksDBOptions.options);
  }

  void openWithRetry(String path) throws PegasusSparkException {
    try {
      booleanRetryer.call(() -> open(path));
    } catch (ExecutionException | RetryException e) {
      throw new PegasusSparkException("sstFileWriter open [" + path + "] failed!", e);
    }
  }

  private boolean open(String path) throws RocksDBException {
    sstFileWriter.open(path);
    return true;
  }

  int writeWithRetry(byte[] key, byte[] value) throws PegasusSparkException {
    try {
      return integerRetryer.call(() -> write(key, value));
    } catch (ExecutionException | RetryException e) {
      throw new PegasusSparkException(
          "sstFileWriter put key-value[key=" + new String(key) + "] failed!", e);
    }
  }

  private int write(byte[] key, byte[] value) throws RocksDBException {
    sstFileWriter.put(key, value);
    return key.length + value.length;
  }

  void closeWithRetry() throws PegasusSparkException {
    try {
      booleanRetryer.call(this::close);
    } catch (ExecutionException | RetryException e) {
      throw new PegasusSparkException("sstFileWriter close failed!", e);
    }
  }

  public boolean close() throws RocksDBException {
    sstFileWriter.finish();
    return true;
  }
}
