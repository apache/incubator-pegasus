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

import io.netty.util.concurrent.Future;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusClientFactory;
import org.apache.pegasus.client.PegasusClientInterface;
import org.apache.pegasus.client.PegasusTableInterface;
import org.apache.pegasus.client.request.BatchWithResponse;
import org.apache.pegasus.client.request.Get;
import org.apache.pegasus.client.request.GetBatch;
import org.apache.pegasus.client.request.Set;
import org.apache.pegasus.client.request.SetBatch;

public class BatchSample {

  // A simple example shows how to use implemented Batch interface.
  public void batch() throws PException {
    String tableName = "temp";
    PegasusClientInterface client = PegasusClientFactory.getSingletonClient();
    PegasusTableInterface table = client.openTable(tableName);

    List<Set> sets = new ArrayList<>();
    sets.add(
        new Set(
            "hashKeySet_1".getBytes(StandardCharsets.UTF_8),
            "sortKeySet1".getBytes(StandardCharsets.UTF_8),
            "valueSet1".getBytes(StandardCharsets.UTF_8)));
    sets.add(
        new Set(
                "hashKeySet_2".getBytes(StandardCharsets.UTF_8),
                "sortKeySet2".getBytes(StandardCharsets.UTF_8),
                "valueSet2".getBytes(StandardCharsets.UTF_8))
            .withTTLSeconds(1000));

    new SetBatch(table, 1000).commit(sets);

    List<Get> gets = new ArrayList<>();
    gets.add(
        new Get(
            "hashKeySet_1".getBytes(StandardCharsets.UTF_8),
            "sortKeySet1".getBytes(StandardCharsets.UTF_8)));
    gets.add(
        new Get(
            "hashKeySet_2".getBytes(StandardCharsets.UTF_8),
            "sortKeySet2".getBytes(StandardCharsets.UTF_8)));

    List<Pair<PException, byte[]>> getResultsWithExp = new ArrayList<>();
    GetBatch getBatch = new GetBatch(table, 1000);
    getBatch.commitWaitAllComplete(gets, getResultsWithExp);

    PegasusClientFactory.closeSingletonClient();
  }

  // A simple example shows how to use Batch interface to send custom request
  public void batchCustom() throws PException {
    class Increment {
      public final byte[] hashKey;
      public final byte[] sortKey;
      final long value;

      private Increment(byte[] hashKey, byte[] sortKey, long value) {
        this.hashKey = hashKey;
        this.sortKey = sortKey;
        this.value = value;
      }
    }

    String tableName = "temp";
    PegasusTableInterface table = PegasusClientFactory.getSingletonClient().openTable(tableName);

    table.del(
        "hashKeyIncr1".getBytes(StandardCharsets.UTF_8),
        "sortKeyIncr1".getBytes(StandardCharsets.UTF_8),
        1000);

    List<Increment> increments = new ArrayList<>();
    increments.add(
        new Increment(
            "hashKeyIncr1".getBytes(StandardCharsets.UTF_8),
            "sortKeyIncr1".getBytes(StandardCharsets.UTF_8),
            1));
    increments.add(
        new Increment(
            "hashKeyIncr1".getBytes(StandardCharsets.UTF_8),
            "sortKeyIncr1".getBytes(StandardCharsets.UTF_8),
            2));

    List<Long> incrResults = new ArrayList<>();

    BatchWithResponse<Increment, Long> incrementBatch =
        new BatchWithResponse<Increment, Long>(table, 1000) {
          @Override
          protected Future<Long> asyncCommit(Increment increment) {
            return table.asyncIncr(increment.hashKey, increment.sortKey, increment.value, timeout);
          }
        };

    incrementBatch.commit(increments, incrResults);

    PegasusClientFactory.closeSingletonClient();
  }
}
