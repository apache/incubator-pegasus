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
package org.apache.pegasus.client;

import io.netty.util.concurrent.Future;
import java.io.Closeable;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author shenyuannan
 *     <p>This class provides interfaces to scan data of a specified table.
 */
public interface PegasusScannerInterface extends Closeable {
  /**
   * Get the next item.
   *
   * @return item like {@literal <<hashKey, sortKey>, value>}; null returned if scan completed.
   * @throws PException
   */
  Pair<Pair<byte[], byte[]>, byte[]> next() throws PException;

  /**
   * Judge whether scan completed
   *
   * @return Return false if no data left, return true means some data left.
   * @throws PException
   */
  boolean hasNext() throws PException;

  /**
   * Get the next item asynchronously.
   *
   * @return A future for current op.
   *     <p>Future return: On success: if scan haven't reached the end then return the kv-pair, else
   *     return null. On failure: a throwable, which is an instance of PException.
   */
  Future<Pair<Pair<byte[], byte[]>, byte[]>> asyncNext();

  /** Close the scanner. Should be called when scan completed. */
  @Override
  void close();
}
