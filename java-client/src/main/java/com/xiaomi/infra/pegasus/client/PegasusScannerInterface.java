// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import io.netty.util.concurrent.Future;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author shenyuannan
 *     <p>This class provides interfaces to scan data of a specified table.
 */
public interface PegasusScannerInterface {
  /**
   * Get the next item.
   *
   * @return item like {@literal <<hashKey, sortKey>, value>}; null returned if scan completed.
   * @throws PException
   */
  public Pair<Pair<byte[], byte[]>, byte[]> next() throws PException;

  /**
   * Get the next item asynchronously.
   *
   * @return A future for current op.
   *     <p>Future return: On success: if scan haven't reach the end then return the kv-pair, else
   *     return null. On failure: a throwable, which is an instance of PException.
   */
  public Future<Pair<Pair<byte[], byte[]>, byte[]>> asyncNext();

  /** Close the scanner. Should be called when scan completed. */
  public void close();
}
