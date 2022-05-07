// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

/**
 * @author qinzuoyan
 *     <p>Multi-get options.
 */
public class MultiGetOptions {
  public boolean startInclusive = true; // if the startSortKey is included
  public boolean stopInclusive = false; // if the stopSortKey is included
  public FilterType sortKeyFilterType = FilterType.FT_NO_FILTER; // filter type for sort key
  public byte[] sortKeyFilterPattern = null; // filter pattern for sort key
  public boolean noValue = false; // only fetch hash_key and sort_key, but not fetch value
  public boolean reverse = false; // if search in reverse direction

  public MultiGetOptions() {}

  public MultiGetOptions(MultiGetOptions o) {
    startInclusive = o.startInclusive;
    stopInclusive = o.stopInclusive;
    sortKeyFilterType = o.sortKeyFilterType;
    sortKeyFilterPattern = o.sortKeyFilterPattern;
    noValue = o.noValue;
    reverse = o.reverse;
  }
}
