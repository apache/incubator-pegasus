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
package org.apache.pegasus.tools;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A tool class to support simple flow control.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * FlowController cntl = new FlowController(qps);
 * while (true) {
 *   cntl.getToken(); // call getToken before operation
 *   client.set(...);
 * }
 * cntl.stop();
 * }</pre>
 */
public class FlowController {

  private final int qps;
  private int[] slots;
  private int next;
  private final AtomicInteger token;
  private boolean stopped;

  /** @param qps QPS to control. should > 0. */
  public FlowController(int qps) {
    this.qps = qps;
    this.slots = new int[10];
    int base = qps / 10;
    for (int i = 0; i < 10; i++) {
      slots[i] = base;
    }
    int remain = qps % 10;
    for (int i = 0; i < 10 && remain > 0; i++) {
      slots[i]++;
      remain--;
    }
    this.next = 1;
    this.token = new AtomicInteger(slots[0]);
    this.stopped = false;
    new Thread(
            new Runnable() {
              @Override
              public void run() {
                while (!stopped) {
                  try {
                    Thread.sleep(100);
                  } catch (InterruptedException e) {
                  }
                  synchronized (token) {
                    token.set(slots[next]);
                    token.notifyAll();
                  }
                  next++;
                  if (next >= 10) next = 0;
                }
              }
            })
        .start();
  }

  /**
   * Call getToken() to do flow control when send request. The method will block for some time if
   * QPS limit is reached to control the flow.
   */
  public void getToken() {
    int t = token.decrementAndGet();
    while (!stopped && t < 0) {
      synchronized (token) {
        try {
          token.wait(100);
        } catch (InterruptedException e) {
        }
      }
      t = token.decrementAndGet();
    }
  }

  /** Should call stop after use done. */
  public void stop() {
    this.stopped = true;
  }
}
