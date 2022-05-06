// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
package com.xiaomi.infra.pegasus.client.request;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public class MultiSet implements Serializable {

  private static final long serialVersionUID = -2236077975122224688L;

  public final byte[] hashKey;
  public final List<Pair<byte[], byte[]>> values;

  public int ttlSeconds;

  public MultiSet(byte[] hashKey) {
    this(hashKey, new ArrayList<>());
  }

  public MultiSet(byte[] hashKey, List<Pair<byte[], byte[]>> values) {
    checkArguments(hashKey, values);
    this.hashKey = hashKey;
    this.values = values;
    this.ttlSeconds = 0;
  }

  public MultiSet add(byte[] sortKey, byte[] value) {
    values.add(Pair.of(sortKey, value));
    return this;
  }

  public MultiSet withTTLSeconds(int ttlSeconds) {
    assert ttlSeconds > 0;
    this.ttlSeconds = ttlSeconds;
    return this;
  }

  private void checkArguments(byte[] hashKey, List<Pair<byte[], byte[]>> values) {
    assert (hashKey != null && hashKey.length > 0 && hashKey.length < 0xFFFF)
        : "hashKey != null && hashKey.length > 0 && hashKey.length < 0xFFFF";
    assert values != null : "values != null";
  }
}
