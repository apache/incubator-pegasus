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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.apps.mutate;
import org.apache.pegasus.apps.mutate_operation;
import org.apache.pegasus.base.blob;
import org.apache.pegasus.tools.Tools;

/**
 * @author huangwei
 *     <p>Mutations in check_and_mutate. All the SET operations in the same Mutations will calculate
 *     the data expire time by TTL using the same beginning timestamp when invoting the
 *     checkAndMutate() method.
 */
public class Mutations {
  private final List<mutate> muList;
  // Pair<index_in_muList, ttlSeconds>
  private final List<Pair<Integer, Integer>> ttlList;

  public Mutations() {
    muList = new ArrayList<mutate>();
    ttlList = new ArrayList<Pair<Integer, Integer>>();
  }

  public void set(byte[] sortKey, byte[] value, int ttlSeconds) {
    blob sortKeyBlob = (sortKey == null ? null : new blob(sortKey));
    blob valueBlob = (value == null ? null : new blob(value));

    // set_expire_ts_seconds will be set when checkAndMutate() gets the mutations (by calling
    // getMutations())
    muList.add(new mutate(mutate_operation.MO_PUT, sortKeyBlob, valueBlob, 0));
    if (ttlSeconds != 0) {
      ttlList.add(Pair.of(muList.size() - 1, ttlSeconds));
    }
  }

  public void set(byte[] sortKey, byte[] value) {
    blob sortKeyBlob = (sortKey == null ? null : new blob(sortKey));
    blob valueBlob = (value == null ? null : new blob(value));
    muList.add(new mutate(mutate_operation.MO_PUT, sortKeyBlob, valueBlob, 0));
  }

  public void del(byte[] sortKey) {
    blob sortKeyBlob = (sortKey == null ? null : new blob(sortKey));
    muList.add(new mutate(mutate_operation.MO_DELETE, sortKeyBlob, null, 0));
  }

  public List<mutate> getMutations() {
    int currentTime = (int) Tools.epoch_now();
    for (Pair<Integer, Integer> pair : ttlList) {
      muList.get(pair.getLeft()).set_expire_ts_seconds = pair.getRight() + currentTime;
    }
    return Collections.unmodifiableList(muList);
  }

  public boolean isEmpty() {
    return muList.isEmpty();
  }
}
