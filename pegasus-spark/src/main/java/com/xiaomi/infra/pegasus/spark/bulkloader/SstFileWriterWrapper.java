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
