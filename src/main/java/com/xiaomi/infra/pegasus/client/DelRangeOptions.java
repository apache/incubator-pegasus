// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.
package com.xiaomi.infra.pegasus.client;

public class DelRangeOptions {
  public byte[] nextSortKey = null;
  public boolean startInclusive = true; // if the startSortKey is included
  public boolean stopInclusive = false; // if the stopSortKey is included
  public FilterType sortKeyFilterType = FilterType.FT_NO_FILTER; // filter type for sort key
  public byte[] sortKeyFilterPattern = null; // filter pattern for sort key

  public DelRangeOptions() {}

  public DelRangeOptions(DelRangeOptions o) {
    nextSortKey = o.nextSortKey;
    startInclusive = o.startInclusive;
    stopInclusive = o.stopInclusive;
    sortKeyFilterType = o.sortKeyFilterType;
    sortKeyFilterPattern = o.sortKeyFilterPattern;
  }
}
