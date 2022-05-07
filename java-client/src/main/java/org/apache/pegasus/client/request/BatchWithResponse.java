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

import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pegasus.client.PException;
import org.apache.pegasus.client.PegasusTableInterface;
import org.apache.pegasus.example.BatchSample;

/**
 * This class is used for sending batched requests with response, default implementations including
 * {@link GetBatch}, {@link MultiGetBatch} and so on, user can implement custom class to send more
 * types of request with response in batch, such as {@link PegasusTableInterface#incr(byte[],
 * byte[], long, int)}, the more usage can see {@link BatchSample}
 *
 * @param <Request> generic type for request
 * @param <Response> generic type for response
 */
public abstract class BatchWithResponse<Request, Response>
    extends AbstractBatch<Request, Response> {

  private static final long serialVersionUID = -2458231362347453846L;

  public BatchWithResponse(PegasusTableInterface table, int timeout) {
    super(table, timeout);
  }

  /**
   * send and commit batched requests no-atomically, but terminate immediately if any error occurs.
   *
   * @param requests generic for request
   * @param responses generic for response
   * @throws PException any error occurs will throw exception
   */
  public void commit(List<Request> requests, List<Response> responses) throws PException {
    asyncCommitRequests(requests).waitAllCompleteOrOneFail(responses, timeout);
  }

  /**
   * send and commit batched requests no-atomically, try wait for all requests done if some other
   * error occurs.
   *
   * @param requests generic for request
   * @param responses generic for response, if one request success, the response is pair(null,
   *     result) otherwise is pair(PException, null)
   */
  public void commitWaitAllComplete(
      List<Request> requests, List<Pair<PException, Response>> responses) {
    asyncCommitRequests(requests).waitAllComplete(responses, timeout);
  }
}
