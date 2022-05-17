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

import io.netty.util.concurrent.Future;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;

public class FutureGroup<Result> {
  private List<Future<Result>> asyncTasks;

  public FutureGroup(int initialCapacity) {
    asyncTasks = new ArrayList<>(initialCapacity);
  }

  public void add(Future<Result> task) {
    asyncTasks.add(task);
  }

  public void waitAllCompleteOrOneFail(int timeoutMillis) throws PException {
    waitAllCompleteOrOneFail(null, timeoutMillis);
  }

  /**
   * Waits until all future tasks complete but terminate if one fails.
   *
   * @param results is nullable, each element is the result of the Future.
   */
  public void waitAllCompleteOrOneFail(List<Result> results, int timeoutMillis) throws PException {
    int timeLimit = timeoutMillis;
    long duration;
    for (int i = 0; i < asyncTasks.size(); i++) {
      Future<Result> fu = asyncTasks.get(i);
      try {
        long startTs = System.currentTimeMillis();
        fu.await(timeLimit);
        duration = System.currentTimeMillis() - startTs;
        timeLimit -= duration;
      } catch (Exception e) {
        throw new PException("async task #[" + i + "] await failed: " + e.toString());
      }

      if (timeLimit < 0) {
        throw new PException(
            String.format("async task #[" + i + "] failed: timeout expired (%dms)", timeoutMillis));
      }

      if (fu.isSuccess()) {
        if (results != null) {
          results.add(fu.getNow());
        }
      } else {
        throw new PException("async task #[" + i + "] failed: ", fu.cause());
      }
    }
  }

  /**
   * wait for all requests done even if some error occurs
   *
   * @param results if one request success, it should be pair(null, result), otherwise,
   *     pair(PException, null)
   * @param timeoutMillis timeout
   */
  public void waitAllComplete(List<Pair<PException, Result>> results, int timeoutMillis) {
    assert results != null : "result != null";
    int timeLimit = timeoutMillis;
    long duration;

    for (int i = 0; i < asyncTasks.size(); i++) {
      Future<Result> fu = asyncTasks.get(i);
      long startTs = System.currentTimeMillis();
      try {
        fu.await(timeLimit);
      } catch (Exception e) {
        results.add(
            Pair.of(new PException("async task #[" + i + "] await failed: " + e.toString()), null));
      } finally {
        duration = System.currentTimeMillis() - startTs;
        timeLimit -= duration;
      }

      if (timeLimit < 0) {
        for (int j = i; j < asyncTasks.size(); j++) {
          results.add(
              Pair.of(
                  new PException(
                      String.format(
                          "async task #[" + i + "] failed: timeout expired (%dms)", timeoutMillis)),
                  null));
        }
        break;
      }

      if (fu.isSuccess()) {
        results.add(Pair.of(null, fu.getNow()));
      } else {
        results.add(
            Pair.of(
                new PException("async task #[" + i + "] await failed: " + fu.cause().getMessage()),
                null));
      }
    }
  }
}
