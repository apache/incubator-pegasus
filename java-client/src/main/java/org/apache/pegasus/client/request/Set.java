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
package org.apache.pegasus.client.request;

import java.io.Serializable;

public class Set implements Serializable {

  private static final long serialVersionUID = -2136211461120973309L;

  public final byte[] hashKey;
  public final byte[] sortKey;
  public final byte[] value;

  public int ttlSeconds;

  public Set(byte[] hashKey, byte[] sortKey, byte[] value) {
    assert value != null : "value != null";
    this.hashKey = hashKey;
    this.sortKey = sortKey;
    this.value = value;
    this.ttlSeconds = 0;
  }

  public Set withTTLSeconds(int ttlSeconds) {
    assert ttlSeconds > 0 : "ttlSeconds > 0";
    this.ttlSeconds = ttlSeconds;
    return this;
  }
}
