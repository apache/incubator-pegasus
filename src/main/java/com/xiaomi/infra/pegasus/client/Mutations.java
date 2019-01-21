// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import com.xiaomi.infra.pegasus.apps.mutate;
import com.xiaomi.infra.pegasus.apps.mutate_operation;
import com.xiaomi.infra.pegasus.base.blob;
import com.xiaomi.infra.pegasus.tools.Tools;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

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
