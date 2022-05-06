// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author qinzuoyan
 *     <p>Check-and-set options.
 */
public class CheckAndSetOptions {
  public int setValueTTLSeconds = 0; // time to live in seconds of the set value, 0 means no ttl.
  public boolean returnCheckValue = false; // whether return the check value in results.

  public CheckAndSetOptions() {}

  public CheckAndSetOptions(CheckAndSetOptions o) {
    setValueTTLSeconds = o.setValueTTLSeconds;
    returnCheckValue = o.returnCheckValue;
  }
}
