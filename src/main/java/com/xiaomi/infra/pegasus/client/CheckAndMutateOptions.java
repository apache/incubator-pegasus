// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author huangwei
 *     <p>check_and_mutate options.
 */
public class CheckAndMutateOptions {
  public boolean returnCheckValue = false; // whether return the check value in results.

  public CheckAndMutateOptions() {}

  public CheckAndMutateOptions(CheckAndMutateOptions o) {
    returnCheckValue = o.returnCheckValue;
  }
}
