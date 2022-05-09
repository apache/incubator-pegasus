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
package org.apache.pegasus.rpc;

import org.apache.pegasus.client.TableOptions;

public class InternalTableOptions {
  private final KeyHasher keyHasher;
  private final TableOptions tableOptions;

  public InternalTableOptions(KeyHasher keyHasher, TableOptions tableOptions) {
    this.keyHasher = keyHasher;
    this.tableOptions = tableOptions;
  }

  public KeyHasher keyHasher() {
    return keyHasher;
  }

  public TableOptions tableOptions() {
    return tableOptions;
  }

  public static InternalTableOptions forTest() {
    return new InternalTableOptions(KeyHasher.DEFAULT, new TableOptions());
  }
}
