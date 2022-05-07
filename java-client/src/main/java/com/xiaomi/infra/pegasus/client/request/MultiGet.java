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

public class MultiGet implements Serializable {

  private static final long serialVersionUID = 2068964280456524026L;

  public final byte[] hashKey;
  public final List<byte[]> sortKeys;

  public MultiGet(byte[] hashKey) {
    this(hashKey, new ArrayList<>());
  }

  public MultiGet(byte[] hashKey, List<byte[]> sortKeys) {
    checkArguments(hashKey, sortKeys);
    this.hashKey = hashKey;
    this.sortKeys = sortKeys;
  }

  public MultiGet add(byte[] sortKey) {
    sortKeys.add(sortKey);
    return this;
  }

  private void checkArguments(byte[] hashKey, List<byte[]> sortKeys) {
    assert (hashKey != null && hashKey.length > 0 && hashKey.length < 0xFFFF)
        : "hashKey != null && hashKey.length > 0 && hashKey.length < 0xFFFF";
    assert sortKeys != null : "sortKeys != null";
  }
}
