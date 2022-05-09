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

  public HashKeyData(byte[] hashKey, List<Pair<byte[], byte[]>> values) {
    this.hashKey = hashKey;
    this.values = values;
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
