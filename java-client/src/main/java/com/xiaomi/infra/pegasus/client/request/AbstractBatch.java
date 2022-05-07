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

import com.xiaomi.infra.pegasus.client.FutureGroup;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import io.netty.util.concurrent.Future;
import java.io.Serializable;
import java.util.List;

/**
 * The abstract class is used for {@link Batch} and {@link BatchWithResponse} which is used for
 * sending batched requests
 *
 * @param <Request> generic type for request
 * @param <Response> generic type for response
 */
public abstract class AbstractBatch<Request, Response> implements Serializable {

  private static final long serialVersionUID = -6267381453465488529L;

  protected PegasusTableInterface table;
  protected int timeout;

  AbstractBatch(PegasusTableInterface table, int timeout) {
    this.table = table;
    this.timeout = timeout;
  }

  FutureGroup<Response> asyncCommitRequests(List<Request> requests) {
    assert !requests.isEmpty() : "requests mustn't be empty";
    FutureGroup<Response> futureGroup = new FutureGroup<>(requests.size());
    for (Request request : requests) {
      futureGroup.add(asyncCommit(request));
    }
    return futureGroup;
  }

  protected abstract Future<Response> asyncCommit(Request request);
}
