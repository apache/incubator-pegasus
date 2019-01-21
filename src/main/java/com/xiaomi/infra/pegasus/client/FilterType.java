// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author qinzuoyan
 *     <p>Filter type.
 */
public enum FilterType {
  FT_NO_FILTER(0),
  FT_MATCH_ANYWHERE(1), // match filter string at any position
  FT_MATCH_PREFIX(2), // match filter string at prefix
  FT_MATCH_POSTFIX(3); // match filter string at postfix

  private final int value;

  private FilterType(int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }
}
