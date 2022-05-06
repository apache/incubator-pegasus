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

import com.xiaomi.infra.pegasus.client.PException;
import com.xiaomi.infra.pegasus.client.PegasusTableInterface;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

/**
 * This class is used for sending batched requests without response, default implementations
 * including {@link SetBatch}, {@link MultiSetBatch} and so on, user can implement custom class to
 * send more types of request without response in batch, the more usage can see {@link
 * com.xiaomi.infra.pegasus.example.BatchSample}
 *
 * @param <Request> generic type for request
 */
public abstract class Batch<Request> extends AbstractBatch<Request, Void> {

  private static final long serialVersionUID = 2048811397820338392L;

  public Batch(PegasusTableInterface table, int timeout) {
    super(table, timeout);
  }

  /**
   * send and commit batched requests no-atomically, but terminate immediately if any error occurs.
   *
   * @param requests generic for request
   * @throws PException any error occurs will throw exception
   */
  public void commit(List<Request> requests) throws PException {
    asyncCommitRequests(requests).waitAllCompleteOrOneFail(timeout);
  }

  /**
   * send and commit batched requests no-atomically, try wait for all requests done until timeout
   * even if some other error occurs.
   *
   * @param requests generic for request
   * @param exceptions if one request is failed, the exception will save into exceptions
   * @throws PException throw exception if timeout
   */
  public void commitWaitAllComplete(List<Request> requests, List<PException> exceptions)
      throws PException {
    List<Pair<PException, Void>> responses = new ArrayList<>();
    asyncCommitRequests(requests).waitAllComplete(responses, timeout);
    convertExceptions(responses, exceptions);
  }

  private void convertExceptions(
      List<Pair<PException, Void>> responses, List<PException> exceptions) {
    for (Pair<PException, Void> response : responses) {
      exceptions.add(response.getKey());
    }
  }
}
