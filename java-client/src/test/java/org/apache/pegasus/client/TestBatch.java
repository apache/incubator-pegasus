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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.client.PegasusTableInterface.MultiGetResult;
import org.apache.pegasus.client.request.BatchWithResponse;
import org.apache.pegasus.client.request.Delete;
import org.apache.pegasus.client.request.DeleteBatch;
import org.apache.pegasus.client.request.Get;
import org.apache.pegasus.client.request.GetBatch;
import org.apache.pegasus.client.request.MultiDelete;
import org.apache.pegasus.client.request.MultiDeleteBatch;
import org.apache.pegasus.client.request.MultiGet;
import org.apache.pegasus.client.request.MultiGetBatch;
import org.apache.pegasus.client.request.MultiSet;
import org.apache.pegasus.client.request.MultiSetBatch;
import org.apache.pegasus.client.request.Set;
import org.apache.pegasus.client.request.SetBatch;
import org.junit.jupiter.api.Test;

public class TestBatch {

  @Test
  public void testBatchSetDelGet() throws PException, InterruptedException {
    String tableName = "temp";
    PegasusTableInterface table = PegasusClientFactory.getSingletonClient().openTable(tableName);

    List<Set> sets = new ArrayList<>();
    sets.add(new Set("hashKeySet_1".getBytes(), "sortKeySet1".getBytes(), "valueSet1".getBytes()));
    sets.add(new Set("hashKeySet_2".getBytes(), "sortKeySet2".getBytes(), "valueSet2".getBytes()));
    sets.add(new Set("hashKeySet_3".getBytes(), "sortKeySet3".getBytes(), "valueSet3".getBytes()));
    sets.add(
        new Set("hashKeySet_4".getBytes(), "sortKeySet4".getBytes(), "valueSet4WithTTL".getBytes())
            .withTTLSeconds(10));
    new SetBatch(table, 1000).commit(sets);

    List<Delete> deletes = new ArrayList<>();
    deletes.add(new Delete("hashKeySet_1".getBytes(), "sortKeySet1".getBytes()));
    deletes.add(new Delete("hashKeySet_2".getBytes(), "sortKeySet2".getBytes()));
    new DeleteBatch(table, 1000).commit(deletes);

    List<Get> gets = new ArrayList<>();
    gets.add(new Get("hashKeySet_1".getBytes(), "sortKeySet1".getBytes()));
    gets.add(new Get("hashKeySet_2".getBytes(), "sortKeySet2".getBytes()));
    gets.add(new Get("hashKeySet_3".getBytes(), "sortKeySet3".getBytes()));
    gets.add(new Get("hashKeySet_4".getBytes(), "sortKeySet4".getBytes()));

    List<byte[]> getResults = new ArrayList<>();
    GetBatch getBatch = new GetBatch(table, 1000);
    getBatch.commit(gets, getResults);

    assertNull(getResults.get(0));
    assertNull(getResults.get(1));
    assertEquals("valueSet3", new String(getResults.get(2)));
    assertEquals("valueSet4WithTTL", new String(getResults.get(3)));

    Thread.sleep(11000);

    List<Pair<PException, byte[]>> getResultsWithExp = new ArrayList<>();
    getBatch.commitWaitAllComplete(gets, getResultsWithExp);
    assertNull(getResultsWithExp.get(2).getKey());
    assertEquals("valueSet3", new String(getResultsWithExp.get(2).getRight()));
    assertNull(getResultsWithExp.get(3).getRight());

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBatchMultiSetDelGet() throws PException {
    String tableName = "temp";
    PegasusTableInterface table = PegasusClientFactory.getSingletonClient().openTable(tableName);

    List<MultiSet> multiSets = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      MultiSet multiSet = new MultiSet(("hashKeyMultiSet" + i).getBytes());
      for (int j = 0; j < 3; j++) {
        multiSet.add(("sortKeyMultiSet" + j).getBytes(), ("valueMultiSet" + j).getBytes());
      }
      multiSets.add(multiSet);
    }
    new MultiSetBatch(table, 1000).commit(multiSets);

    List<MultiDelete> multiDeletes = new ArrayList<>();
    for (int i = 0; i < 2; i++) {
      MultiDelete multiDelete = new MultiDelete(("hashKeyMultiSet" + i).getBytes());
      for (int j = 0; j < 3; j++) {
        multiDelete.add(("sortKeyMultiSet" + j).getBytes());
      }
      multiDeletes.add(multiDelete);
    }
    new MultiDeleteBatch(table, 1000).commit(multiDeletes);

    List<MultiGet> multiGets = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      MultiGet multiGet = new MultiGet(("hashKeyMultiSet" + i).getBytes());
      for (int j = 0; j < 3; j++) {
        multiGet.add(("sortKeyMultiSet" + j).getBytes());
      }
      multiGets.add(multiGet);
    }

    List<MultiGetResult> multiGetResults = new ArrayList<>();
    MultiGetBatch multiGetBatch = new MultiGetBatch(table, 1000);
    multiGetBatch.commit(multiGets, multiGetResults);

    assertEquals(0, multiGetResults.get(0).getValues().size());
    assertEquals(0, multiGetResults.get(1).getValues().size());
    assertTrue(multiGetResults.get(2).isAllFetched());
    for (int i = 0; i < 3; i++) {
      assertEquals(
          "sortKeyMultiSet" + i, new String(multiGetResults.get(2).getValues().get(i).getKey()));
      assertEquals(
          "valueMultiSet" + i, new String(multiGetResults.get(2).getValues().get(i).getRight()));
    }

    List<Pair<PException, MultiGetResult>> multiGetResultsWithExp = new ArrayList<>();
    multiGetBatch.commitWaitAllComplete(multiGets, multiGetResultsWithExp);
    for (int i = 0; i < 3; i++) {
      assertNull(multiGetResultsWithExp.get(2).getLeft());
      assertEquals(
          "sortKeyMultiSet" + i,
          new String(multiGetResultsWithExp.get(2).getRight().getValues().get(i).getKey()));
      assertEquals(
          "valueMultiSet" + i,
          new String(multiGetResultsWithExp.get(2).getRight().getValues().get(i).getRight()));
    }

    PegasusClientFactory.closeSingletonClient();
  }

  @Test
  public void testBatchCustomRequest() throws PException {
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

    table.del("hashKeyIncr1".getBytes(), "sortKeyIncr1".getBytes(), 1000);

    List<Increment> increments = new ArrayList<>();
    increments.add(new Increment("hashKeyIncr1".getBytes(), "sortKeyIncr1".getBytes(), 1));
    increments.add(new Increment("hashKeyIncr1".getBytes(), "sortKeyIncr1".getBytes(), 2));

    List<Long> incrResults = new ArrayList<>();

    BatchWithResponse<Increment, Long> incrementBatch =
        new BatchWithResponse<Increment, Long>(table, 1000) {
          @Override
          protected Future<Long> asyncCommit(Increment increment) {
            return table.asyncIncr(increment.hashKey, increment.sortKey, increment.value, timeout);
          }
        };

    incrementBatch.commit(increments, incrResults);

    assertEquals(1, incrResults.get(0).longValue());
    assertEquals(3, incrResults.get(1).longValue());

    PegasusClientFactory.closeSingletonClient();
  }
}
