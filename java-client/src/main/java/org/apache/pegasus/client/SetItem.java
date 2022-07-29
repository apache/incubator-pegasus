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

import java.io.Serializable;

public class SetItem implements Serializable {

  private static final long serialVersionUID = -8889678176839129753L;

  public byte[] hashKey = null;
  public byte[] sortKey = null;
  public byte[] value = null;
  public int ttlSeconds = 0; // 0 means no ttl

  public SetItem() {}

  public SetItem(byte[] hashKey, byte[] sortKey, byte[] value, int ttlSeconds) {
    this.hashKey = hashKey;
    this.sortKey = sortKey;
    this.value = value;
    this.ttlSeconds = ttlSeconds;
  }

  public SetItem(byte[] hashKey, byte[] sortKey, byte[] value) {
    this.hashKey = hashKey;
    this.sortKey = sortKey;
    this.value = value;
    this.ttlSeconds = 0;
  }
}
