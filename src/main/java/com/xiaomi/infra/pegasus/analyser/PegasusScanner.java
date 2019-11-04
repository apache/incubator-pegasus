package com.xiaomi.infra.pegasus.analyser;

import org.apache.commons.lang3.tuple.Pair;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

public class PegasusScanner implements AutoCloseable {

  private RocksIterator rocksIterator;
  private RocksDB rocksDB;

  public PegasusScanner(RocksDB rocksDB, RocksIterator rocksIterator) {
    this.rocksDB = rocksDB;
    this.rocksIterator = rocksIterator;
  }

  public void seekToFirst() {
    rocksIterator.seekToFirst();
    if (rocksIterator.key() == null || rocksIterator.key().length < 2) {
      rocksIterator.next();
    }
  }

  public boolean isValid() {
    return rocksIterator.isValid();
  }

  public void next() {
    rocksIterator.next();
  }

  public PegasusKey key() {
    Pair<byte[], byte[]> keyPair = Utils.restoreKey(rocksIterator.key());
    return new PegasusKey(keyPair.getLeft(), keyPair.getRight());
  }

  public byte[] value() {
    return Utils.restoreValue(rocksIterator.value());
  }

  @Override
  public void close() {
    rocksIterator.close();
    rocksDB.close();
  }
}
