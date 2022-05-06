// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.tools;

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
  private AtomicInteger token;
  private boolean stopped;

  /** @param qps_ QPS to control. should > 0. */
  public FlowController(int qps_) {
    this.qps = qps_;
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
