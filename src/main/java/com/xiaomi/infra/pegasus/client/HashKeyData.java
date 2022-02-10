// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

import java.util.*;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author qinzuoyan
 *     <p>Store data under the same hashKey.
 */
public class HashKeyData {
  public boolean allFetched = true;
  public byte[] hashKey = null;
  public List<Pair<byte[], byte[]>> values = null; // List{sortKey, value}

  public HashKeyData() {}

  public HashKeyData(byte[] hashKey) {
    this.hashKey = hashKey;
  }

  public HashKeyData(boolean allFetched, byte[] hashKey, List<Pair<byte[], byte[]>> values) {
    this.allFetched = allFetched;
    this.hashKey = hashKey;
    this.values = values;
  }

  public void addData(byte[] sortKey, byte[] value) {
    if (values == null) values = new ArrayList<Pair<byte[], byte[]>>();
    values.add(Pair.of(sortKey, value));
  }
}
