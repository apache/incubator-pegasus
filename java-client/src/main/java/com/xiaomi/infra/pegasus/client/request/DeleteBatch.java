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

import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import io.netty.util.concurrent.Future;

public class DeleteBatch extends Batch<Delete> {

  private static final long serialVersionUID = -3749505015438921947L;

  public DeleteBatch(PegasusTableInterface table, int timeout) {
    super(table, timeout);
  }

  @Override
  protected Future<Void> asyncCommit(Delete delete) {
    return table.asyncDel(delete.hashKey, delete.sortKey, timeout);
  }
}
