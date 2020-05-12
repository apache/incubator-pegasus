// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.rpc;

/** TableOptions is the internal options for opening a Pegasus table. */
public class TableOptions {
  private final KeyHasher keyHasher;
  private final int backupRequestDelayMs;

  public KeyHasher keyHasher() {
    return this.keyHasher;
  }

  public int backupRequestDelayMs() {
    return this.backupRequestDelayMs;
  }

  public static TableOptions forTest() {
    return new TableOptions(KeyHasher.DEFAULT, 0);
  }

  public TableOptions(KeyHasher h, int backupRequestDelay) {
    this.keyHasher = h;
    this.backupRequestDelayMs = backupRequestDelay;
  }
}
