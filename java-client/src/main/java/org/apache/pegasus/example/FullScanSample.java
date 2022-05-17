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
package org.apache.pegasus.example;

import java.io.UnsupportedEncodingException;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.client.*;

/** A simple example shows how to use full scan. */
public class FullScanSample {

  private static final String tableName = "temp";

  public static void main(String[] args) throws PException, UnsupportedEncodingException {
    PegasusClientInterface client = PegasusClientFactory.createClient(ClientOptions.create());

    // Set up the scanners.
    ScanOptions scanOptions = new ScanOptions();
    scanOptions.batchSize = 20;

    List<PegasusScannerInterface> scanners =
        client.getUnorderedScanners(tableName, 16, scanOptions);
    System.out.printf("opened %d scanners%n", scanners.size());

    // Iterates sequentially.
    for (PegasusScannerInterface scanner : scanners) {
      int cnt = 0;
      long start = System.currentTimeMillis();
      while (true) {
        Pair<Pair<byte[], byte[]>, byte[]> pair = scanner.next();
        if (null == pair) break;
        Pair<byte[], byte[]> keys = pair.getLeft();
        byte[] hashKey = keys.getLeft();
        byte[] sortKey = keys.getRight();
        cnt++;
        // hashKey/sortKey/value is not necessarily encoded via UTF-8.
        // It may be ASCII encoded or using any types of encoding.
        // Here we assume they are UTF-8 strings.
        System.out.printf(
            "hashKey = %s, sortKey = %s, value = %s%n",
            new String(hashKey, "UTF-8"),
            new String(sortKey, "UTF-8"),
            new String(pair.getValue(), "UTF-8"));
      }
      System.out.printf("scanning %d rows costs %d ms%n", cnt, System.currentTimeMillis() - start);
    }
    client.close();
  }
}
