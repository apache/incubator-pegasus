package com.xiaomi.infra;

import com.xiaomi.infra.service.db.PegasusScanner;
import java.io.Serializable;
import org.rocksdb.RocksDBException;

public interface PegasusClientInterface extends Serializable {

  /**
   * @param pid pegasus partition id
   * @return PegasusScanner
   * @throws RocksDBException
   */
  public PegasusScanner getScanner(int pid) throws RocksDBException;

  /**
   * @param pid pegasus partition id
   * @return pegasus data count
   * @throws RocksDBException
   */
  public int getDataCount(int pid) throws RocksDBException;

  /** @return pegasus partition id */
  public int getPartitionCount();
}
